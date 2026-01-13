// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    convert::{TryFrom, TryInto},
    fmt, fs,
    fs::{File, OpenOptions},
    io::{BufWriter, Read, Seek, SeekFrom, Write},
    ops,
    path::{Path, PathBuf},
    sync::Arc,
};

use bitflags::bitflags;
use bytes::{Buf, BufMut, Bytes};
use collections::{HashMap, HashSet};
use kvenginepb as pb;
use log_wrappers::Value as LogValue;
use protobuf::Message;
use tikv_util::{
    box_try,
    codec::number::{U8_SIZE, U32_SIZE, U64_SIZE},
    config::ReadableSize,
    mpsc::{Receiver, Sender, paired_callback},
    sys::thread::StdThreadBuildWrapper,
    time::Instant,
};

use crate::{
    IoContext,
    dfs::FileType,
    error::Result as KvResult,
    metrics::{META_PACK_ACTION_COUNTER_VEC, META_PACKER_METAS_COUNT},
    table::{
        Error, Result, sstable,
        sstable::{SsTable, validate_checksum},
    },
};

const FORCE_COMPACT_WRITTEN_BYTES: u64 = 100 * 1024 * 1024; // 100MB

bitflags! {
    #[derive(Default)]
    struct TinyMetaFlags: u8 {
        const SEGMENT_OFFSETS = 1 << 0;
    }
}

#[derive(Clone)]
pub struct TinyMeta {
    pub file_id: u64,
    pub footer_and_properties: Bytes,      // Footer + properties.
    pub segment_offsets: Option<Vec<u32>>, // For IA only.
}

impl fmt::Debug for TinyMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TinyMeta")
            .field("id", &self.file_id)
            .field("footer_and_properties", &self.footer_and_properties.len())
            .field(
                "segment_offsets",
                &self.segment_offsets.as_ref().map(Vec::len),
            )
            .finish()
    }
}

impl TinyMeta {
    pub fn try_convert_to(self, ftype: FileType) -> TypedTinyMeta {
        match ftype {
            FileType::Sst => SstTinyMeta::try_from(self)
                .map(TypedTinyMeta::Sst)
                .unwrap_or_default(),
            _ => TypedTinyMeta::None,
        }
    }

    fn read_footer(&self, footer_length: usize) -> Result<Bytes> {
        let data_len = self.footer_and_properties.len();
        let Some(off) = data_len.checked_sub(footer_length) else {
            error!("TinyMeta: read_footer: invalid file size";
                "file" => self.file_id, "data_len" => data_len, "footer_length" => footer_length);
            return Err(Error::InvalidFileSize);
        };
        Ok(self.footer_and_properties.slice(off..))
    }

    pub fn marshal(&self) -> Result<Vec<u8>> {
        let include_segment_offsets = self.segment_offsets.is_some();
        let segment_offsets_len = self.segment_offsets.as_ref().map_or(0, Vec::len);
        let flag = include_segment_offsets
            .then_some(TinyMetaFlags::SEGMENT_OFFSETS)
            .unwrap_or_default();

        // file_id + flag + footer_and_properties.len + footer_and_properties +
        // [segment_offsets.len + segment_offsets]
        let cap = U64_SIZE
            + U8_SIZE
            + U32_SIZE
            + self.footer_and_properties.len()
            + include_segment_offsets as usize * (U32_SIZE + segment_offsets_len * U32_SIZE);
        let mut buf = Vec::with_capacity(cap);
        buf.put_u64_le(self.file_id);
        buf.put_u8(flag.bits());
        buf.put_u32_le(box_try!(self.footer_and_properties.len().try_into()));
        buf.put_slice(&self.footer_and_properties);
        if let Some(segment_offsets) = &self.segment_offsets {
            buf.put_u32_le(box_try!(segment_offsets.len().try_into()));
            for offset in segment_offsets {
                buf.put_u32_le(*offset);
            }
        }
        Ok(buf)
    }

    pub fn unmarshal(buf: &mut &[u8]) -> Result<Self> {
        let expected_len = U64_SIZE + U8_SIZE + U32_SIZE; // file_id + flag + footer_and_properties.len
        if buf.remaining() < expected_len {
            return Err(Error::CorruptedMetaPack(format!(
                "length mismatch: {} < {}",
                buf.remaining(),
                expected_len
            )));
        }
        let file_id = buf.get_u64_le();
        let flag = TinyMetaFlags::from_bits_truncate(buf.get_u8());
        let footer_and_properties_len = buf.get_u32_le() as usize;
        if buf.remaining() < footer_and_properties_len {
            return Err(Error::CorruptedMetaPack(format!(
                "data length mismatch: {} < {}",
                buf.remaining(),
                footer_and_properties_len
            )));
        }
        let footer_and_properties = Bytes::copy_from_slice(&buf[..footer_and_properties_len]);
        buf.advance(footer_and_properties_len);

        let segment_offsets = if !flag.contains(TinyMetaFlags::SEGMENT_OFFSETS) {
            None
        } else {
            if buf.remaining() < U32_SIZE {
                return Err(Error::CorruptedMetaPack(format!(
                    "segment offsets length mismatch: {} < {}",
                    buf.remaining(),
                    U32_SIZE
                )));
            }
            let segment_offsets_len = buf.get_u32_le() as usize;
            let segment_offsets_bytes =
                segment_offsets_len.checked_mul(U32_SIZE).ok_or_else(|| {
                    Error::CorruptedMetaPack(format!(
                        "segment offsets size overflow: {} * {}",
                        segment_offsets_len, U32_SIZE
                    ))
                })?;
            if buf.remaining() < segment_offsets_bytes {
                return Err(Error::CorruptedMetaPack(format!(
                    "segment offsets data length mismatch: {} < {}",
                    buf.remaining(),
                    segment_offsets_bytes
                )));
            }
            let mut segment_offsets = Vec::with_capacity(segment_offsets_len);
            for _ in 0..segment_offsets_len {
                segment_offsets.push(buf.get_u32_le());
            }
            Some(segment_offsets)
        };

        Ok(TinyMeta {
            file_id,
            footer_and_properties,
            segment_offsets,
        })
    }
}

#[derive(Default, Clone, Debug)]
pub enum TypedTinyMeta {
    #[default]
    None,
    Sst(SstTinyMeta),
}

impl TypedTinyMeta {
    pub fn meta_size(&self) -> Option<u64> {
        match self {
            TypedTinyMeta::None => None,
            TypedTinyMeta::Sst(sst_meta) => Some(sst_meta.meta_size()),
        }
    }

    pub fn file_size(&self) -> Option<u64> {
        match self {
            TypedTinyMeta::None => None,
            TypedTinyMeta::Sst(sst_meta) => Some(sst_meta.file_size()),
        }
    }

    pub fn into_sst(self) -> Option<SstTinyMeta> {
        match self {
            TypedTinyMeta::None => None,
            TypedTinyMeta::Sst(sst_meta) => Some(sst_meta),
        }
    }

    pub fn as_sst(&self) -> Option<&SstTinyMeta> {
        match self {
            TypedTinyMeta::None => None,
            TypedTinyMeta::Sst(sst_meta) => Some(sst_meta),
        }
    }

    pub fn into_inner(self) -> Option<TinyMeta> {
        match self {
            TypedTinyMeta::None => None,
            TypedTinyMeta::Sst(sst_meta) => Some(sst_meta.inner),
        }
    }
}

#[derive(Clone)]
pub struct SstTinyMeta {
    pub inner: TinyMeta,
    pub footer: sstable::Footer,
}

impl ops::Deref for SstTinyMeta {
    type Target = TinyMeta;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl fmt::Debug for SstTinyMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SstTinyMeta")
            .field("inner", &self.inner)
            .field("footer", &self.footer)
            .finish()
    }
}

impl TryFrom<TinyMeta> for SstTinyMeta {
    type Error = Error;

    fn try_from(tiny_meta: TinyMeta) -> Result<Self> {
        let footer_data = tiny_meta.read_footer(SsTable::footer_size())?;
        let mut footer = sstable::Footer::default();
        footer.unmarshal(&footer_data);
        if !footer.is_match() {
            error!("SstTinyMeta: footer not match";
                "file" => tiny_meta.file_id, "footer" => LogValue::value(&footer_data));
            return Err(Error::CorruptedMetaPack(format!(
                "SstTinyMeta: footer not match: {}",
                tiny_meta.file_id
            )));
        }

        Ok(SstTinyMeta {
            inner: tiny_meta,
            footer,
        })
    }
}

impl From<SstTinyMeta> for TinyMeta {
    fn from(sst_tiny_meta: SstTinyMeta) -> TinyMeta {
        sst_tiny_meta.inner
    }
}

impl SstTinyMeta {
    fn convert_segment_offsets(file_id: u64, offsets: &[u64]) -> Option<Vec<u32>> {
        let mut converted = Vec::with_capacity(offsets.len());
        for &offset in offsets {
            match u32::try_from(offset) {
                Ok(value) => converted.push(value),
                Err(_) => {
                    warn!("SstTinyMeta: segment offset overflow";
                        "file" => file_id, "offset" => offset);
                    return None;
                }
            }
        }
        Some(converted)
    }

    pub fn from_sstable(
        sstable: &SsTable,
        footer: sstable::Footer,
        properties_data: &[u8],
    ) -> Self {
        debug_assert_eq!(sstable.footer(), footer);

        let mut buf = Vec::with_capacity(properties_data.len() + SsTable::footer_size());
        buf.put_slice(properties_data);
        footer.marshal(&mut buf);

        let segment_offsets = sstable
            .try_get_ia_file()
            .and_then(|f| Self::convert_segment_offsets(sstable.id(), &f.segment_offsets))
            .or_else(|| {
                sstable.try_get_auto_ia_file().and_then(|f| {
                    Self::convert_segment_offsets(sstable.id(), &f.ia_file.segment_offsets)
                })
            });

        Self {
            inner: TinyMeta {
                file_id: sstable.id(),
                footer_and_properties: buf.into(),
                segment_offsets,
            },
            footer,
        }
    }

    fn try_read(&self, off: u64, length: usize) -> Option<Bytes> {
        let tiny_meta_off = self.footer.tiny_meta_offset() as u64;
        if off >= tiny_meta_off && off + length as u64 <= self.file_size() {
            let start = (off - tiny_meta_off) as usize;
            Some(self.footer_and_properties.slice(start..start + length))
        } else {
            None
        }
    }

    pub fn get_footer_and_properties(&self) -> Result<(sstable::Footer, Bytes)> {
        let props_off = self.footer.properties_offset;
        let props_len = self.footer.properties_len(self.file_size() as usize);

        let Some(props_data) = self.try_read(props_off as u64, props_len) else {
            warn!("SstTinyMeta: get_properties_data: invalid properties range";
                "file_id" => self.file_id, "props_off" => props_off, "props_len" => props_len);
            debug_assert!(false);
            return Err(Error::CorruptedMetaPack(format!(
                "invalid properties range: off={} len={}",
                props_off, props_len
            )));
        };

        box_try!(validate_checksum(self.file_id, &props_data, &self.footer));
        Ok((self.footer, props_data))
    }

    #[inline]
    pub fn file_size(&self) -> u64 {
        self.footer.tiny_meta_offset() as u64 + self.inner.footer_and_properties.len() as u64
    }

    #[inline]
    pub fn meta_size(&self) -> u64 {
        self.file_size() - self.footer.meta_offset() as u64
    }
}

const META_PACK_V1: u32 = 1;

struct MetaPackHeader {
    // Use protobuf for easy future extension.
    inner: pb::MetaPackHeader,
}

impl Default for MetaPackHeader {
    fn default() -> Self {
        MetaPackHeader {
            inner: pb::MetaPackHeader {
                version: META_PACK_V1,
                ..Default::default()
            },
        }
    }
}

impl MetaPackHeader {
    fn marshal(&self) -> Vec<u8> {
        let header_len = self.inner.compute_size();
        let cap = U32_SIZE + header_len as usize;
        let mut buf = Vec::with_capacity(cap);
        buf.put_u32_le(header_len);
        self.inner.write_to_vec(&mut buf).unwrap();
        debug_assert_eq!(buf.len(), cap);
        buf
    }

    fn unmarshal(buf: &mut &[u8]) -> Result<Self> {
        if buf.remaining() < U32_SIZE {
            return Err(Error::CorruptedMetaPack(format!(
                "header length mismatch: {} < {}",
                buf.remaining(),
                U32_SIZE
            )));
        }

        let header_len = buf.get_u32_le() as usize;
        if buf.remaining() < header_len {
            return Err(Error::CorruptedMetaPack(format!(
                "header data length mismatch: {} < {}",
                buf.remaining(),
                header_len
            )));
        }

        let mut pb_header = pb::MetaPackHeader::new();
        pb_header
            .merge_from_bytes(&buf[..header_len])
            .map_err(|e| Error::CorruptedMetaPack(format!("failed to parse header: {}", e)))?;
        if pb_header.version != META_PACK_V1 {
            return Err(Error::CorruptedMetaPack(format!(
                "unsupported meta pack version: {}",
                pb_header.version
            )));
        }
        buf.advance(header_len);

        Ok(MetaPackHeader { inner: pb_header })
    }
}

#[derive(Clone, Default)]
pub struct MetaPackReader {
    core: Arc<ReaderCore>,
}

impl ops::Deref for MetaPackReader {
    type Target = ReaderCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl MetaPackReader {
    pub fn open(path: &Path, writable: bool) -> KvResult<Self> {
        let (core, _) = ReaderCore::open_core(path, writable)?;
        Ok(MetaPackReader {
            core: Arc::new(core),
        })
    }
}

#[derive(Default)]
pub struct ReaderCore {
    metas: HashMap<u64 /* file_id */, TinyMeta>,
}

impl ReaderCore {
    fn open_core(path: &Path, writable: bool) -> KvResult<(Self, u64 /* pack_size */)> {
        let start_time = Instant::now_coarse();

        let mut metas = HashMap::default();
        let mut f = OpenOptions::new()
            .read(true)
            .write(writable)
            .create(writable)
            .open(path)
            .ctx(format!("meta_reader.open.{}", path.display()))?;

        let mut buf = vec![];
        f.read_to_end(&mut buf).ctx("meta_reader.read")?;
        let mut slice: &[u8] = &buf;
        let pack_size = slice.remaining();

        if let Err(err) = MetaPackHeader::unmarshal(&mut slice) {
            if pack_size != 0 {
                warn!("TinyMetaReader: unmarshal header error: {:?}", err;
                    "pack_size" => pack_size);
                if writable {
                    f.set_len(0).ctx("meta_reader.truncate")?;
                    f.seek(SeekFrom::Start(0)).ctx("meta_reader.seek_header")?;
                }
            }

            if writable {
                let header = MetaPackHeader::default();
                let header_bytes = header.marshal();
                f.write_all(&header_bytes).ctx("meta_reader.write_header")?;
            }
            return Ok((ReaderCore::default(), pack_size as u64));
        }

        while slice.has_remaining() {
            let last_remaining = slice.remaining();
            let meta = match TinyMeta::unmarshal(&mut slice) {
                Ok(meta) => meta,
                Err(err) => {
                    // The last entry may be incomplete due to crash during writing.
                    let read_len = pack_size - last_remaining;
                    warn!(
                        "meta_reader.unmarshal error: {:?}, truncate len: {}",
                        err, read_len
                    );
                    if writable {
                        f.set_len(read_len as u64)
                            .ctx("meta_reader.truncate_incomplete")?;
                    }
                    break;
                }
            };
            metas.insert(meta.file_id, meta);
        }

        info!("TinyMetaReader: load pack";
            "metas" => metas.len(), "pack_size" => pack_size,
            "takes" => ?start_time.saturating_elapsed());

        Ok((ReaderCore { metas }, pack_size as u64))
    }

    pub fn get(&self, file_id: u64) -> Option<TinyMeta> {
        self.metas.get(&file_id).cloned()
    }

    pub fn len(&self) -> usize {
        self.metas.len()
    }

    pub fn is_empty(&self) -> bool {
        self.metas.is_empty()
    }
}

pub enum MetaPackTask {
    Pack(TinyMeta),
    Stop(Box<dyn FnOnce(Result<()>) + Send>),
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct MetaPackConfig {
    /// Whether to enable tables meta pack.
    ///
    /// When enabled, tables' tiny meta (footer + properties + ...) will be
    /// packed into a single file to reduce the IOPS and increase the speed
    /// for kvengine startup.
    pub enabled: bool,

    /// Maximum number of pending tasks in the channel.
    pub max_pending: usize,

    /// Try to compact after append `trigger_compact_threshold` bytes
    /// of meta data.
    pub try_compact_threshold: ReadableSize,

    /// The ratio of `all_files_in_pack / all_files_in_kvengine` when trigger a
    /// compaction.
    pub compact_ratio: f64,
}

impl Default for MetaPackConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_pending: 1_000_000, // 100MB as each meta is about 100 bytes.
            try_compact_threshold: ReadableSize::mb(1),
            compact_ratio: 4.0,
        }
    }
}

#[derive(Default, Clone)]
pub struct CompactKeeper(Arc<CompactKeeperCore>);

#[derive(Default)]
pub struct CompactKeeperCore {
    counter: Arc<()>,
}

pub struct CompactPauseGuard(Arc<()>);

impl CompactPauseGuard {
    pub fn is_paused(&self) -> bool {
        Arc::strong_count(&self.0) > 1
    }
}

impl CompactKeeperCore {
    pub fn pause(&self) -> CompactPauseGuard {
        CompactPauseGuard(self.counter.clone())
    }

    pub fn is_paused(&self) -> bool {
        Arc::strong_count(&self.counter) > 1
    }
}

impl ops::Deref for CompactKeeper {
    type Target = CompactKeeperCore;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

pub struct MetaPacker {
    config: MetaPackConfig,
    path: PathBuf,

    worker_tx: Sender<MetaPackTask>,
    worker_rx: Option<Receiver<MetaPackTask>>,

    reader: Option<MetaPackReader>,
    init_metas_count: usize,

    compact_keeper: CompactKeeper,
}

impl MetaPacker {
    pub fn stop(&self) {
        let (cb, rx) = paired_callback();
        if self.worker_tx.send(MetaPackTask::Stop(cb)).is_ok() {
            match rx.recv() {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    error!("MetaPacker stop error: {:?}", err);
                    debug_assert!(false);
                }
                Err(err) => {
                    warn!("MetaPacker stop callback recv error: {:?}", err);
                }
            }
        }
    }

    pub fn compact_keeper(&self) -> CompactKeeper {
        self.compact_keeper.clone()
    }
}

#[derive(Clone)]
pub struct MetaPackScheduler {
    tx: Sender<MetaPackTask>,
}

impl MetaPackScheduler {
    pub fn try_pack(&self, meta: TinyMeta) {
        if let Err(err) = self.tx.try_send(MetaPackTask::Pack(meta))
            && !err.is_disconnected()
        {
            warn!("MetaPacker pack request dropped: {:?}", err);
        }
    }
}

impl MetaPacker {
    pub fn new(path: PathBuf, config: MetaPackConfig) -> KvResult<Self> {
        let reader = MetaPackReader::open(&path, true)?;
        let init_metas_count = reader.len();
        let (tx, rx) = tikv_util::mpsc::bounded(config.max_pending);
        Ok(MetaPacker {
            config,
            path,
            worker_tx: tx,
            worker_rx: Some(rx),
            reader: Some(reader),
            init_metas_count,
            compact_keeper: CompactKeeper::default(),
        })
    }

    pub fn take_reader(&mut self) -> Option<MetaPackReader> {
        self.reader.take()
    }

    pub fn get_scheduler(&self) -> MetaPackScheduler {
        MetaPackScheduler {
            tx: self.worker_tx.clone(),
        }
    }

    pub fn start_worker(&mut self, kv: crate::Engine) -> KvResult<()> {
        if self.worker_rx.is_none() {
            // The worker has already started.
            return Ok(());
        }

        let f = OpenOptions::new()
            .append(true)
            .open(&self.path)
            .ctx(format!("meta_worker.open.{}", self.path.display()))?;
        let writer = BufWriter::new(f);

        let mut worker = MetaPackWorker {
            config: self.config.clone(),
            path: self.path.clone(),
            rx: self.worker_rx.take().unwrap(),
            kv,
            compact_keeper: self.compact_keeper.clone(),
            writer: Some(writer),
            metas_count: self.init_metas_count,
            written_bytes_after_try_compact: 0,
        };
        std::thread::Builder::new()
            .name("meta-packer".to_string())
            .spawn_wrapper(move || {
                worker.run();
            })
            .unwrap();
        Ok(())
    }
}

struct MetaPackWorker {
    config: MetaPackConfig,
    path: PathBuf,
    rx: Receiver<MetaPackTask>,
    kv: crate::Engine,
    compact_keeper: CompactKeeper,

    writer: Option<BufWriter<File>>,

    metas_count: usize,
    written_bytes_after_try_compact: u64,
}

#[derive(Default)]
struct ExistedFiles {
    sst_files: HashSet<u64>,
    sst_ia_files: HashSet<u64>,
    blacklist: HashSet<u64>,
}

impl ExistedFiles {
    fn len(&self) -> usize {
        // Ignore the overlapped files (IaAutoFiles only, which should be rare).
        self.sst_files.len() + self.sst_ia_files.len() + self.blacklist.len()
    }

    fn contains(&self, tiny_meta: &TinyMeta) -> bool {
        let file_id = tiny_meta.file_id;
        if self.blacklist.contains(&file_id) {
            return true;
        }
        if tiny_meta.segment_offsets.is_some() {
            self.sst_ia_files.contains(&file_id)
        } else {
            self.sst_files.contains(&file_id)
        }
    }
}

impl MetaPackWorker {
    fn run(&mut self) {
        self.refresh_metrics();

        while let Ok(task) = self.rx.recv() {
            match task {
                MetaPackTask::Pack(meta) => {
                    if let Err(err) = self.handle_pack(meta) {
                        error!("MetaPacker pack error: {:?}", err);
                        continue;
                    }

                    self.try_compact();
                }
                MetaPackTask::Stop(cb) => {
                    let res = self.handle_stop();
                    cb(res);
                    return;
                }
            };
        }
    }

    fn handle_pack(&mut self, meta: TinyMeta) -> KvResult<()> {
        if let Some(writer) = self.writer.as_mut() {
            let data = box_try!(meta.marshal());
            writer.write_all(&data).ctx("handle_pack.write")?;

            self.written_bytes_after_try_compact += data.len() as u64;
            self.metas_count += 1;
            self.refresh_metrics();
            META_PACK_ACTION_COUNTER_VEC.pack.inc();
        }
        Ok(())
    }

    fn handle_stop(&mut self) -> Result<()> {
        if let Some(mut writer) = self.writer.take() {
            box_try!(writer.flush());
            box_try!(writer.get_ref().sync_data());

            info!("MetaPacker stopped");
        }
        Ok(())
    }

    fn precheck_compact(&self) -> bool {
        Self::precheck_compact_impl(
            self.written_bytes_after_try_compact,
            &self.config,
            &self.compact_keeper,
        )
    }

    fn precheck_compact_impl(
        written_bytes_after_try_compact: u64,
        config: &MetaPackConfig,
        compact_keeper: &CompactKeeper,
    ) -> bool {
        written_bytes_after_try_compact >= FORCE_COMPACT_WRITTEN_BYTES
            || (written_bytes_after_try_compact >= config.try_compact_threshold.0
                && !compact_keeper.is_paused())
    }

    fn try_compact(&mut self) {
        if !self.precheck_compact() {
            return;
        }
        self.written_bytes_after_try_compact = 0;

        let exist_files = self.collect_exist_files();
        if (self.metas_count as f64) < (exist_files.len() as f64) * self.config.compact_ratio {
            return;
        }

        if let Err(err) = self.compact(exist_files) {
            warn!("MetaPacker compact error: {:?}", err);
        }
    }

    fn compact(&mut self, exist_files: ExistedFiles) -> KvResult<()> {
        let start_time = Instant::now_coarse();

        if let Some(writer) = self.writer.as_mut() {
            writer.flush().ctx("compact.writer_flush")?;
        } else {
            // Stopped.
            return Ok(());
        }

        let (reader, origin_pack_size) = box_try!(ReaderCore::open_core(&self.path, false));
        let origin_metas_count = reader.metas.len();

        let compact_path = self.path.with_extension("compact");
        let mut compact_writer = {
            let f = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&compact_path)
                .ctx(format!("compact.open_compact.{}", self.path.display()))?;
            BufWriter::new(f)
        };

        let mut new_metas_count = 0usize;
        let mut new_pack_size = 0u64;
        let mut drop_metas_count = 0usize;

        let header = MetaPackHeader::default();
        let header_bytes = header.marshal();
        new_pack_size += header_bytes.len() as u64;
        compact_writer
            .write_all(&header_bytes)
            .ctx("compact.write_header")?;

        for tiny_meta in reader.metas.values() {
            if exist_files.contains(tiny_meta) {
                match tiny_meta.marshal() {
                    Ok(data) => {
                        compact_writer.write_all(&data).ctx("compact.write")?;

                        new_metas_count += 1;
                        new_pack_size += data.len() as u64;
                    }
                    Err(err) => {
                        warn!("MetaPacker: compact: marshal failed: {:?}", err;
                            "file" => tiny_meta.file_id);
                        debug_assert!(false);
                    }
                };
            } else {
                drop_metas_count += 1;
            }
        }

        compact_writer.flush().ctx("compact.flush")?;
        compact_writer
            .get_ref()
            .sync_data()
            .ctx("compact.sync_data")?;

        fs::rename(compact_path, &self.path).ctx("compact.rename")?;
        if let Some(parent) = self.path.parent() {
            file_system::sync_dir(parent).ctx("compact.sync_dir")?;
        }

        let f = OpenOptions::new()
            .append(true)
            .open(&self.path)
            .ctx(format!("meta_worker.open.{}", self.path.display()))?;
        let writer = BufWriter::new(f);
        self.writer = Some(writer);
        self.metas_count = new_metas_count;

        self.refresh_metrics();
        META_PACK_ACTION_COUNTER_VEC.compact.inc();

        info!("MetaPacker compacted";
            "exist_files" => exist_files.len(),
            "origin_metas" => origin_metas_count,
            "new_metas" => new_metas_count,
            "origin_pack_size" => origin_pack_size,
            "new_pack_size" => new_pack_size,
            "drop_metas" => drop_metas_count,
            "takes" => ?start_time.saturating_elapsed());
        Ok(())
    }

    fn collect_exist_files(&self) -> ExistedFiles {
        let mut exist_files = ExistedFiles::default();

        let blacklist = self.kv.get_files_in_blacklist();
        exist_files.blacklist.extend(blacklist.as_ref());

        let shard_id_vers = self.kv.get_all_shard_id_vers();
        for id_ver in shard_id_vers {
            let Some(shard) = self.kv.get_shard(id_ver.id) else {
                continue;
            };
            let (local_files, ia_files) = shard.get_local_sst_files();
            exist_files.sst_files.extend(local_files);
            exist_files.sst_ia_files.extend(ia_files);
            // TODO: Handle other file types.
        }

        exist_files
    }

    fn refresh_metrics(&self) {
        META_PACKER_METAS_COUNT.set(self.metas_count as i64);
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Deref, sync::Arc};

    use bytes::Bytes;
    use rand::prelude::*;
    use schema::schema::{StorageClass, StorageClassSpec};
    use tempfile::TempDir;

    use super::*;
    use crate::{
        STORAGE_CLASS_KEY, ShardCf, ShardCfBuilder, ShardDataBuilder,
        ia::{ia_file::IaFile, manager::IaManager, util::IaManagerOptionsBuilder},
        table::{
            ChecksumType, InnerKey, NO_COMPRESSION, Value,
            file::InMemFile,
            sstable::{
                BlockCache, SsTable,
                builder::{FOOTER_SIZE, Footer, MAGIC_NUMBER, TABLE_FORMAT_V1},
            },
        },
    };

    #[test]
    fn test_tiny_meta() {
        // Test round-trip marshaling/unmarshaling.
        let footer_and_properties = Bytes::from_static(b"footer and properties data");
        let cases = [
            (98765, Some(vec![3, 7, 11])),
            (123, None),
            (456, Some(vec![])),
        ];

        for (file_id, segment_offsets) in cases {
            let original = TinyMeta {
                file_id,
                footer_and_properties: footer_and_properties.clone(),
                segment_offsets,
            };

            let marshaled = original.marshal().expect("marshal should succeed");
            assert!(!marshaled.is_empty());

            let mut slice: &[u8] = &marshaled;
            let restored = TinyMeta::unmarshal(&mut slice).expect("unmarshal should succeed");

            assert_eq!(restored.file_id, original.file_id);
            assert_eq!(
                restored.footer_and_properties,
                original.footer_and_properties
            );
            assert_eq!(restored.segment_offsets, original.segment_offsets);
            assert!(slice.is_empty()); // All data should be consumed.
        }
    }

    #[test]
    fn test_tiny_meta_unmarshal_insufficient_data() {
        {
            // Test unmarshal with insufficient data
            let mut slice: &[u8] = &[0u8; 5]; // Less than needed (U64_SIZE + U8_SIZE + U32_SIZE = 13)
            let result = TinyMeta::unmarshal(&mut slice);
            assert!(result.is_err());
            match result {
                Err(Error::CorruptedMetaPack(msg)) => {
                    assert!(msg.contains("length mismatch"));
                }
                _ => panic!("Expected CorruptedMetaPack error"),
            }
        }

        {
            // Test unmarshal with data length mismatch
            let mut buf = Vec::new();
            buf.put_u64_le(12345); // file_id
            buf.put_u8(0); // reserved
            buf.put_u32_le(100); // data_len (claiming 100 bytes)
            buf.put_slice(&[1u8; 50]); // But only provide 50 bytes

            let mut slice: &[u8] = &buf;
            let result = TinyMeta::unmarshal(&mut slice);
            assert!(result.is_err());
            match result {
                Err(Error::CorruptedMetaPack(msg)) => {
                    assert!(msg.contains("data length mismatch"));
                }
                _ => panic!("Expected CorruptedMetaPack error"),
            }
        }
    }

    #[test]
    fn test_typed_tiny_meta() {
        // Test TypedTinyMeta::None variant
        let none_meta = TypedTinyMeta::None;
        assert!(none_meta.meta_size().is_none());
        assert!(none_meta.file_size().is_none());
        assert!(none_meta.as_sst().is_none());
        assert!(none_meta.clone().into_sst().is_none());
        assert!(none_meta.into_inner().is_none());
    }

    #[test]
    fn test_sst_tiny_meta() {
        // Test successful conversion with valid SST data
        let file_id = 12345;
        let (tiny_meta, table_data, table_meta_off) = make_tiny_meta_ext(file_id, 20, false);
        let table_file_size = table_data.len() as u64;

        // Test TypedTinyMeta conversion
        let typed = tiny_meta.clone().try_convert_to(FileType::Sst);
        match &typed {
            TypedTinyMeta::Sst(sst_meta) => {
                assert_eq!(sst_meta.file_id, file_id);
                assert_eq!(
                    sst_meta.footer_and_properties,
                    tiny_meta.footer_and_properties
                );
                assert_eq!(sst_meta.segment_offsets, tiny_meta.segment_offsets);

                assert_eq!(sst_meta.file_size(), table_file_size);
                assert_eq!(sst_meta.meta_size(), table_file_size - table_meta_off);
                assert_eq!(
                    sst_meta.footer.tiny_meta_offset(),
                    table_file_size as u32 - tiny_meta.footer_and_properties.len() as u32
                );
            }
            _ => panic!("Should convert to Sst variant"),
        }
    }

    #[test]
    fn test_sst_tiny_meta_try_from_invalid() {
        // Test failed conversion with invalid SST data
        let tiny_meta = make_invalid_tiny_meta(99999);

        let result = SstTinyMeta::try_from(tiny_meta);
        assert!(result.is_err());
        match result {
            Err(Error::CorruptedMetaPack(msg)) => {
                assert!(msg.contains("footer not match"));
            }
            _ => panic!("Expected CorruptedMetaPack error for invalid footer"),
        }
    }

    #[test]
    fn test_sst_tiny_meta_try_read() {
        let file_id = 77777;
        let tiny_meta = make_tiny_meta(file_id, 20);
        let sst_meta = SstTinyMeta::try_from(tiny_meta).expect("Should convert");

        // Test try_read with valid offset (within tiny meta range)
        let props_off = sst_meta.footer.properties_offset as u64;
        let props_len = sst_meta
            .footer
            .properties_len(sst_meta.file_size() as usize);

        // Should be able to read properties data
        let read_result = sst_meta.try_read(props_off, props_len);
        assert!(read_result.is_some());
        let read_data = read_result.unwrap();
        assert_eq!(read_data.len(), props_len);

        // Test try_read with offset outside range
        let invalid_read = sst_meta.try_read(sst_meta.file_size() + 100, 10);
        assert!(invalid_read.is_none());

        // Test try_read with offset+length exceeding file size
        let overflow_read = sst_meta.try_read(sst_meta.file_size() - 5, 10);
        assert!(overflow_read.is_none());
    }

    #[test]
    fn test_sst_tiny_meta_get_footer_and_properties() {
        let file_id = 88888;
        let tiny_meta = make_tiny_meta(file_id, 20);
        let sst_meta = SstTinyMeta::try_from(tiny_meta).expect("Should convert");

        // Test get_footer_and_properties
        let result = sst_meta.get_footer_and_properties();
        assert!(result.is_ok());
        let (footer, props_data) = result.unwrap();

        assert!(footer.is_match());
        assert_eq!(footer.magic, MAGIC_NUMBER);
        assert_eq!(footer.table_format_version, TABLE_FORMAT_V1);
        assert_eq!(
            props_data.len(),
            sst_meta
                .footer
                .properties_len(sst_meta.file_size() as usize)
        );
    }

    #[test]
    fn test_meta_pack() {
        let (test_engine, _applier_tx) = crate::tests::new_test_engine();

        let tmp_dir = TempDir::new().unwrap();
        let pack_path = tmp_dir.path().join("meta.pack");

        let config = pack_config();
        let mut packer = MetaPacker::new(pack_path.clone(), config.clone()).unwrap();

        let initial_reader = packer.take_reader().unwrap();
        assert!(packer.take_reader().is_none());
        assert!(initial_reader.is_empty());

        packer.start_worker(test_engine.deref().clone()).unwrap();
        packer.start_worker(test_engine.deref().clone()).unwrap(); // no-op (already started)
        let scheduler = packer.get_scheduler();

        let mut expected = HashMap::default();
        for file_id in 1..=20u64 {
            let meta = make_tiny_meta(file_id, 50);
            expected.insert(file_id, meta.clone());
            scheduler.try_pack(meta);
        }

        packer.stop();

        let mut restarted = MetaPacker::new(pack_path.clone(), config).unwrap();
        let reader = restarted.take_reader().unwrap();
        assert_eq!(reader.len(), expected.len());

        for (file_id, expected_meta) in expected {
            let got = reader.get(file_id).expect("meta should exist");
            assert_eq!(got.file_id, expected_meta.file_id);
            assert_eq!(
                got.footer_and_properties,
                expected_meta.footer_and_properties
            );
            assert_eq!(got.segment_offsets, expected_meta.segment_offsets);

            let sst_meta = SstTinyMeta::try_from(got).expect("sst meta should be valid");
            let (_footer, _props) = sst_meta
                .get_footer_and_properties()
                .expect("checksum should be valid");
        }
    }

    #[test]
    fn test_meta_pack_truncates_incomplete() {
        let (test_engine, _applier_tx) = crate::tests::new_test_engine();

        let tmp_dir = TempDir::new().unwrap();
        let pack_path = tmp_dir.path().join("meta.pack");

        let config = pack_config();
        let mut packer = MetaPacker::new(pack_path.clone(), config.clone()).unwrap();
        packer.start_worker(test_engine.deref().clone()).unwrap();
        let scheduler = packer.get_scheduler();

        let mut expected = HashMap::default();
        for file_id in 1..=10u64 {
            let meta = make_tiny_meta(file_id, 40);
            expected.insert(file_id, meta.clone());
            scheduler.try_pack(meta);
        }
        packer.stop();

        let clean_size = fs::metadata(&pack_path).unwrap().len();

        let incomplete_file_id = 999u64;
        let incomplete = make_tiny_meta(incomplete_file_id, 10).marshal().unwrap();
        assert!(incomplete.len() > 5);

        {
            let mut f = OpenOptions::new().append(true).open(&pack_path).unwrap();
            f.write_all(&incomplete[..5]).unwrap();
            f.sync_data().unwrap();
        }

        let corrupted_size = fs::metadata(&pack_path).unwrap().len();
        assert!(corrupted_size > clean_size);

        let mut restarted = MetaPacker::new(pack_path.clone(), config).unwrap();
        let reader = restarted.take_reader().unwrap();

        let truncated_size = fs::metadata(&pack_path).unwrap().len();
        assert_eq!(truncated_size, clean_size);
        assert!(reader.get(incomplete_file_id).is_none());

        assert_eq!(reader.len(), expected.len());
        for (file_id, expected_meta) in expected {
            let got = reader.get(file_id).expect("meta should exist");
            assert_eq!(
                got.footer_and_properties,
                expected_meta.footer_and_properties
            );
            assert_eq!(got.segment_offsets, expected_meta.segment_offsets);
            SstTinyMeta::try_from(got).expect("sst meta should be valid");
        }
    }

    #[test]
    fn test_meta_pack_recovers_corrupted_header() {
        let (test_engine, _applier_tx) = crate::tests::new_test_engine();

        let tmp_dir = TempDir::new().unwrap();
        let pack_path = tmp_dir.path().join("meta.pack");

        fs::write(&pack_path, vec![0xABu8; 128]).unwrap();
        assert!(fs::metadata(&pack_path).unwrap().len() > 0);

        let config = pack_config();
        let header_len = MetaPackHeader::default().marshal().len() as u64;

        let mut packer = MetaPacker::new(pack_path.clone(), config.clone()).unwrap();
        assert_eq!(fs::metadata(&pack_path).unwrap().len(), header_len);

        let reader_after_repair = MetaPackReader::open(&pack_path, false).unwrap();
        assert!(reader_after_repair.is_empty());

        packer.start_worker(test_engine.deref().clone()).unwrap();
        let scheduler = packer.get_scheduler();

        let mut expected = HashMap::default();
        for file_id in 1..=10u64 {
            let meta = make_tiny_meta(file_id, 20);
            expected.insert(file_id, meta.clone());
            scheduler.try_pack(meta);
        }
        packer.stop();

        let mut restarted = MetaPacker::new(pack_path, config).unwrap();
        let reader = restarted.take_reader().unwrap();
        assert_eq!(reader.len(), expected.len());

        for (file_id, expected_meta) in expected {
            let got = reader.get(file_id).expect("meta should exist");
            assert_eq!(
                got.footer_and_properties,
                expected_meta.footer_and_properties
            );
            assert_eq!(got.segment_offsets, expected_meta.segment_offsets);
            SstTinyMeta::try_from(got).expect("sst meta should be valid");
        }
    }

    #[test]
    fn test_meta_pack_compact() {
        ::test_util::init_log_for_test();
        let (test_engine, _applier_tx) = crate::tests::new_test_engine();

        let shard = test_engine.get_shard(1).unwrap();
        let (meta1, data1, _) = make_tiny_meta_ext(1, 30, false);
        let (meta2, data2, _) = make_tiny_meta_ext(2, 30, false);
        let (meta3, data3, _) = make_tiny_meta_ext(3, 30, false);
        let t1 = make_sstable_from_data(1, data1);
        let t2 = make_sstable_from_data(2, data2);
        let t3 = make_sstable_from_data(3, data3);
        let mut cf_builder = ShardCfBuilder::new(0);
        cf_builder.add_table(t1.clone(), 1);
        cf_builder.add_table(t2.clone(), 1);
        cf_builder.add_table(t3.clone(), 1);
        let mut builder = ShardDataBuilder::new(shard.get_data());
        builder.set_cfs([cf_builder.build(), ShardCf::new(1), ShardCf::new(2)]);
        shard.set_data(builder.build());

        let tmp_dir = TempDir::new().unwrap();
        let pack_path = tmp_dir.path().join("meta.pack");

        let mut metas = Vec::new();
        let mut keep = HashMap::default();
        for file_id in 1..=20u64 {
            let meta = match file_id {
                1 => meta1.clone(),
                2 => meta2.clone(),
                3 => meta3.clone(),
                _ => make_tiny_meta_ext(file_id, 30, false).0,
            };
            if file_id <= 3 {
                keep.insert(file_id, meta.clone());
            }
            metas.push(meta);
        }

        let total_bytes: usize = metas.iter().map(|m| m.marshal().unwrap().len()).sum();

        let config = MetaPackConfig {
            enabled: true,
            max_pending: 1024,
            try_compact_threshold: ReadableSize(total_bytes as u64),
            compact_ratio: 4.0,
        };

        let mut packer = MetaPacker::new(pack_path.clone(), config.clone()).unwrap();
        packer.start_worker(test_engine.deref().clone()).unwrap();
        let scheduler = packer.get_scheduler();

        for meta in metas {
            scheduler.try_pack(meta);
        }
        packer.stop();

        let header_len = MetaPackHeader::default().marshal().len() as u64;
        let kept_bytes: u64 = keep
            .values()
            .map(|m| m.marshal().unwrap().len() as u64)
            .sum();
        let expected_pack_size = header_len + kept_bytes;
        assert_eq!(fs::metadata(&pack_path).unwrap().len(), expected_pack_size);

        let mut restarted = MetaPacker::new(pack_path.clone(), config).unwrap();
        let reader = restarted.take_reader().unwrap();
        assert_eq!(reader.len(), keep.len());
        for (file_id, expected_meta) in keep {
            let got = reader.get(file_id).expect("meta should exist");
            assert_eq!(
                got.footer_and_properties,
                expected_meta.footer_and_properties
            );
            assert_eq!(got.segment_offsets, expected_meta.segment_offsets);
            let sst_meta = SstTinyMeta::try_from(got).unwrap();
            sst_meta.get_footer_and_properties().unwrap();
        }
        for file_id in 4..=20u64 {
            assert!(reader.get(file_id).is_none());
        }

        // Transit to IA
        {
            let sc_spec: StorageClassSpec = StorageClass::Ia.into();
            shard.set_property(STORAGE_CLASS_KEY, &sc_spec.marshal());

            let ia_rt = tokio::runtime::Builder::new_multi_thread()
                .thread_name("ia-mgr")
                .enable_all()
                .worker_threads(1)
                .build()
                .unwrap();
            let options = IaManagerOptionsBuilder::default().build().unwrap();
            let mgr = IaManager::new(options, test_engine.fs.clone(), None, ia_rt.into()).unwrap();

            let (ia_meta1, ia_data1, _) = make_tiny_meta_ext(1, 30, true);
            let (ia_meta21, ia_data21, _) = make_tiny_meta_ext(21, 30, true);
            let ia_t1 = convert_local_sst_to_ia(&make_sstable_from_data(1, ia_data1), mgr.clone());
            let ia_t21 = convert_local_sst_to_ia(&make_sstable_from_data(21, ia_data21), mgr);

            let mut ia_cf_builder = ShardCfBuilder::new(0);
            ia_cf_builder.add_table(ia_t1, 1);
            ia_cf_builder.add_table(ia_t21, 1);
            let mut builder = ShardDataBuilder::new(shard.get_data());
            builder.set_cfs([ia_cf_builder.build(), ShardCf::new(1), ShardCf::new(2)]);
            shard.set_data(builder.build());

            let mut ia_metas = Vec::new();
            let mut ia_keep = HashMap::default();
            for meta in [ia_meta1, ia_meta21] {
                ia_keep.insert(meta.file_id, meta.clone());
                ia_metas.push(meta);
            }

            let ia_total_bytes: usize = ia_metas.iter().map(|m| m.marshal().unwrap().len()).sum();
            let ia_config = MetaPackConfig {
                enabled: true,
                max_pending: 1024,
                try_compact_threshold: ReadableSize(ia_total_bytes as u64),
                compact_ratio: 1.0,
            };

            let mut packer = MetaPacker::new(pack_path.clone(), ia_config.clone()).unwrap();
            packer.start_worker(test_engine.deref().clone()).unwrap();
            let scheduler = packer.get_scheduler();

            for meta in ia_metas {
                scheduler.try_pack(meta);
            }
            packer.stop();

            let mut restarted = MetaPacker::new(pack_path, ia_config).unwrap();
            let reader = restarted.take_reader().unwrap();
            assert_eq!(reader.len(), ia_keep.len());
            for (file_id, expected_meta) in ia_keep {
                let got = reader.get(file_id).expect("meta should exist");
                assert_eq!(
                    got.footer_and_properties,
                    expected_meta.footer_and_properties
                );
                assert_eq!(got.segment_offsets, expected_meta.segment_offsets);
            }
            for file_id in 2..=20u64 {
                assert!(reader.get(file_id).is_none());
            }
        }
    }

    #[test]
    fn test_precheck_compact_impl_compact_keeper_pause() {
        let threshold = ReadableSize::kb(1).0;
        let config = MetaPackConfig {
            enabled: true,
            max_pending: 1,
            try_compact_threshold: ReadableSize(threshold),
            compact_ratio: 1.0,
        };
        let keeper = CompactKeeper::default();

        let cases = vec![
            ("below threshold", threshold - 1, false, false),
            ("at threshold unpaused", threshold, false, true),
            ("at threshold paused", threshold, true, false),
            (
                "force compact paused",
                FORCE_COMPACT_WRITTEN_BYTES,
                true,
                true,
            ),
        ];

        for (name, written_bytes, paused, expected) in cases {
            let _pause_guard = paused.then(|| keeper.pause());
            assert_eq!(
                MetaPackWorker::precheck_compact_impl(written_bytes, &config, &keeper),
                expected,
                "{name}"
            );
        }
    }

    #[test]
    fn test_meta_pack_compact_pause() {
        ::test_util::init_log_for_test();
        let (test_engine, _applier_tx) = crate::tests::new_test_engine();

        let tmp_dir = TempDir::new().unwrap();
        let pack_path = tmp_dir.path().join("meta.pack");

        let meta1 = make_tiny_meta(1, 10);
        let meta2 = make_tiny_meta(2, 10);
        let threshold_bytes = std::cmp::min(
            meta1.marshal().unwrap().len(),
            meta2.marshal().unwrap().len(),
        );

        let config = MetaPackConfig {
            enabled: true,
            max_pending: 1024,
            try_compact_threshold: ReadableSize(threshold_bytes as u64),
            compact_ratio: 1.0,
        };

        {
            let mut packer = MetaPacker::new(pack_path.clone(), config.clone()).unwrap();
            let _pause_guard = packer.compact_keeper().pause();
            packer.start_worker(test_engine.deref().clone()).unwrap();
            let scheduler = packer.get_scheduler();

            // Should trigger compact but be paused.
            scheduler.try_pack(meta1);
            packer.stop();

            let reader_before = MetaPackReader::open(&pack_path, false).unwrap();
            assert_eq!(reader_before.len(), 1);
            assert!(reader_before.get(1).is_some());
        }

        {
            let mut packer = MetaPacker::new(pack_path.clone(), config.clone()).unwrap();
            let pause_guard = packer.compact_keeper().pause();
            packer.start_worker(test_engine.deref().clone()).unwrap();
            let scheduler = packer.get_scheduler();

            drop(pause_guard);
            // Should trigger compact.
            scheduler.try_pack(meta2);
            packer.stop();
        }

        let mut restarted = MetaPacker::new(pack_path, config).unwrap();
        let reader = restarted.take_reader().unwrap();
        assert!(reader.is_empty());
    }

    fn pack_config() -> MetaPackConfig {
        MetaPackConfig {
            enabled: true,
            max_pending: 1024,
            try_compact_threshold: ReadableSize::gb(1),
            compact_ratio: 1024.0,
        }
    }

    fn make_sstable_from_data(file_id: u64, data: Bytes) -> SsTable {
        let file = InMemFile::new(file_id, data);
        SsTable::new(Arc::new(file), BlockCache::None, None).unwrap()
    }

    fn make_sstable_data(
        file_id: u64,
        n: usize,
        key_len: usize,
        val_len: usize,
        multi_ver: bool,
    ) -> (
        Bytes, // file_data
        u64,   // tiny_meta_off
        u64,   // meta_off
    ) {
        let mut rng = thread_rng();

        let mut builder = sstable::Builder::new(
            file_id,
            4096,
            NO_COMPRESSION,
            0,
            ChecksumType::default(),
            None,
        );
        let mut val = vec![0; val_len];
        let mut ver = n as u64;
        let mut i = 0;
        for _ in 0..n {
            let key = format!("{:0key_len$}", i).into_bytes();
            rng.fill_bytes(val.as_mut_slice());
            let value_buf = Value::encode_buf(0u8, &[0], ver, &val);
            let value = Value::decode(&value_buf);
            builder.add(InnerKey::from_inner_buf(&key), &value, None);

            if multi_ver && rng.gen_ratio(1, 4) {
                ver -= 1;
            } else {
                i += 1;
                ver = n as u64;
            }
        }

        let mut buf = Vec::with_capacity(builder.estimated_size());
        let res = builder.finish(0, &mut buf);
        let file_data = Bytes::from(buf);
        (
            file_data,
            res.tiny_meta_offset as u64,
            res.meta_offset as u64,
        )
    }

    // Helper function to create a valid TinyMeta
    fn make_tiny_meta(file_id: u64, n: usize) -> TinyMeta {
        let with_offsets = thread_rng().gen_ratio(1, 2);
        let (tiny_meta, ..) = make_tiny_meta_ext(file_id, n, with_offsets);
        tiny_meta
    }

    fn make_tiny_meta_ext(
        file_id: u64,
        n: usize,
        with_offsets: bool,
    ) -> (
        TinyMeta,
        Bytes, // table_data
        u64,   // meta_off
    ) {
        let (table_data, tiny_meta_off, meta_off) = make_sstable_data(file_id, n, 7, 10, false);
        let mut rng = thread_rng();
        let amount = rng.gen_range(0..5);
        let meta_off_u32 = u32::try_from(meta_off).expect("meta_off should fit in u32");
        let segment_offsets =
            with_offsets.then(|| (0..meta_off_u32).choose_multiple(&mut rng, amount));
        (
            TinyMeta {
                file_id,
                footer_and_properties: table_data.slice(tiny_meta_off as usize..),
                segment_offsets,
            },
            table_data,
            meta_off,
        )
    }

    fn convert_local_sst_to_ia(t: &SsTable, mgr: IaManager) -> SsTable {
        let meta_data = t
            .file()
            .read(
                t.meta_offset() as u64,
                t.size() as usize - t.meta_offset() as usize,
            )
            .unwrap();
        let meta_file = InMemFile::new(t.id(), meta_data);
        let ia_file = IaFile::open_for_sst(t.id(), Arc::new(meta_file), mgr, None).unwrap();
        SsTable::new(Arc::new(ia_file), BlockCache::None, None).unwrap()
    }

    // Helper function to create a TinyMeta with invalid SST footer data
    fn make_invalid_tiny_meta(file_id: u64) -> TinyMeta {
        // Create footer with wrong magic number
        let mut footer = Footer::default();
        footer.magic = 0xDEADBEEF; // Invalid magic
        footer.table_format_version = TABLE_FORMAT_V1;

        // Use same structure as valid footer for consistency
        let data_size = 100;
        let index_size = 50;
        let properties_size = 80;

        footer.old_data_offset = 0;
        footer.index_offset = data_size as u32;
        footer.old_index_offset = 0;
        footer.aux_index_offset = 0;
        footer.properties_offset = (data_size + index_size) as u32;
        footer.compression_type = 0;
        footer.checksum_type = 1;

        let mut footer_data = Vec::new();
        footer.marshal(&mut footer_data);

        let total_size = data_size + index_size + properties_size + FOOTER_SIZE;
        let mut sst_data = vec![0u8; total_size];
        let footer_start = total_size - FOOTER_SIZE;
        sst_data[footer_start..].copy_from_slice(&footer_data);

        TinyMeta {
            file_id,
            footer_and_properties: Bytes::from(sst_data),
            segment_offsets: None,
        }
    }
}

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    alloc::{self, Layout},
    cmp,
    fs::{File, OpenOptions},
    io::Read,
    mem,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    ptr::NonNull,
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicUsize, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};

use bytes::{Buf, BufMut};
use file_system::open_direct_file;
use tikv_util::{
    error, info,
    mpsc::{Receiver, Sender},
    time::Instant,
    warn,
};

use crate::{config::Config, load::wal_exists, write_batch::PeerBatch, *};

pub(crate) const EPOCH_SNAPSHOT_LEN: u32 = 8;

pub const BATCH_HEADER_SIZE: usize = 4 /* epoch_id */ + 4 /* checksum */ + 4 /* batch_len */;
pub(crate) const INITIAL_BUF_SIZE: usize = 8 * 1024 * 1024;

#[derive(PartialEq, Copy, Clone)]
pub enum WriterType {
    Sync,
    Async,
    CliMode, // Used for cli tools like full restoration.
}

/// `DmaBuffer` is a buffer used for direct I/O that follows the alignment
/// restrictions on the length and address of user-space buffers.
///
/// The typical usage is:
///
/// ```ignore
/// let mut buf = DmaBuffer::new(16*1024);
/// let data = b"data";
/// buf.ensure_space(data.len());
/// let chunk = unsafe { buf.chunk_mut() };
/// chunk.copy_from_slice(data);
/// unsafe { buf.advance_mut(data.len()) };
/// buf.pad_to_align();
/// write(buf.as_ref());
/// ```
pub(crate) struct DmaBuffer {
    data: NonNull<u8>,
    layout: Layout,
    len: usize,
}

unsafe impl Send for DmaBuffer {}

impl DmaBuffer {
    const DMA_ALIGN: usize = 4096;

    pub(crate) fn new(cap: usize) -> Self {
        debug_assert!(0 < cap && cap <= isize::MAX as usize);
        let layout = Layout::from_size_align(cap, Self::DMA_ALIGN)
            .unwrap()
            .pad_to_align();
        let data = unsafe { alloc::alloc(layout) };
        let data = NonNull::new(data).expect("memory allocation success");
        Self {
            data,
            layout,
            len: 0,
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn capacity(&self) -> usize {
        self.layout.size()
    }

    /// Shortens the buffer, keeping the first `len` elements and dropping
    /// the rest.
    ///
    /// If `len` is greater than the buffer's current length, this has no
    /// effect.
    fn truncate(&mut self, len: usize) {
        self.len = cmp::min(self.len, len);
    }

    /// Ensures enough space for `size`.
    fn ensure_space(&mut self, size: usize) {
        if self.capacity() - self.len >= size {
            return;
        }
        let require_cap = self
            .len
            .checked_add(size)
            .expect("capacity shouldn't overflow");
        let new_cap = cmp::max(self.layout.size() * 2, require_cap);
        let new_layout = Layout::from_size_align(new_cap, Self::DMA_ALIGN)
            .unwrap()
            .pad_to_align();
        let data = unsafe { alloc::realloc(self.data.as_ptr(), self.layout, new_layout.size()) };
        self.data = NonNull::new(data).expect("memory allocation success");
        self.layout = new_layout;
    }

    /// Pads the length of buf to the alignment. It doesn't pad zeros.
    pub(crate) fn pad_to_align(&mut self) {
        self.len = Self::aligned_len(self.len);
        assert!(self.len <= self.capacity());
    }

    /// Returns a mutable slice starting at the current position.
    ///
    /// This function is unsafe because the returned byte slice may represent
    /// uninitialized memory.
    unsafe fn chunk_mut(&mut self) -> &mut [u8] {
        &mut std::slice::from_raw_parts_mut(self.data.as_ptr(), self.capacity())[self.len..]
    }

    /// Advances the internal cursor of the Buffer.
    ///
    /// The next call to `chunk_mut` will return a slice starting `cnt` bytes
    /// further into the underlying buf.
    ///
    /// This function is unsafe because there is no guarantee that the bytes
    /// being advanced past have been initialized.
    unsafe fn advance_mut(&mut self, cnt: usize) {
        self.len += cnt;
        assert!(self.len <= self.capacity());
    }

    pub(crate) fn aligned_len(len: usize) -> usize {
        len.wrapping_add(Self::DMA_ALIGN - 1) & !(Self::DMA_ALIGN - 1)
    }
}

impl Drop for DmaBuffer {
    fn drop(&mut self) {
        unsafe {
            alloc::dealloc(self.data.as_ptr(), self.layout);
        }
    }
}

impl AsRef<[u8]> for DmaBuffer {
    fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data.as_ptr(), self.len) }
    }
}

impl AsMut<[u8]> for DmaBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.data.as_ptr(), self.len) }
    }
}

/// Magic Number of the WAL file. It's picked by running
///    echo rfengine.wal | sha1sum
/// and taking the leading 64 bits.
const WAL_MAGIC_NUMBER: u64 = 0xf126b8135c90588e;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u64)]
pub(crate) enum Version {
    V2 = 2,
}

impl Version {
    fn from(version: u64) -> Result<Version> {
        match version {
            2 => Ok(Version::V2),
            _ => Err(Error::Corruption {
                msg: format!("WAL version mismatch: version {:x}", version),
                epoch_id: 0,
                offset: 0,
                data: vec![],
            }),
        }
    }
}

#[derive(PartialEq, Eq, Debug)]
pub(crate) struct WalHeader {
    pub(crate) version: Version,
    pub(crate) epoch_id: u32,
}

impl WalHeader {
    pub(crate) fn new(version: Version, epoch_id: u32) -> Self {
        Self { version, epoch_id }
    }
}

impl WalHeader {
    pub(crate) const fn len() -> usize {
        DmaBuffer::DMA_ALIGN
    }

    fn encode_to(&self, mut buf: &mut [u8]) {
        assert!(buf.len() >= Self::len());
        buf.put_u64_le(WAL_MAGIC_NUMBER);
        buf.put_u64_le(self.version as u64);
        buf.put_u32_le(self.epoch_id);
    }

    pub(crate) fn decode(mut buf: &[u8]) -> Result<Self> {
        if buf.len() < Self::len() {
            return Err(Error::Corruption {
                msg: format!("WAL header mismatch: len {}", buf.len()),
                epoch_id: 0,
                offset: 0,
                data: buf.to_vec(),
            });
        }
        let magic_number = buf.get_u64_le();
        if magic_number != WAL_MAGIC_NUMBER {
            return Err(Error::Corruption {
                msg: format!("WAL magic number mismatch: magic_number {:x}", magic_number),
                epoch_id: 0,
                offset: 0,
                data: buf.to_vec(),
            });
        }
        let version = Version::from(buf.get_u64_le())?;
        let epoch_id = buf.get_u32_le();
        Ok(Self { version, epoch_id })
    }
}

pub(crate) fn check_wal_header(
    dir: &Path,
    epoch_id: u32,
    epoch_rotate_len: usize,
) -> Result<WalHeader> {
    let filename = wal_file_name(dir, epoch_id, epoch_rotate_len);
    if let Ok(mut file) = File::open(filename) {
        let mut buf = vec![0u8; WalHeader::len()];
        if file.read_exact(&mut buf).is_ok() {
            return match WalHeader::decode(&buf) {
                Ok(header) => {
                    if header.epoch_id != epoch_id {
                        return Err(Error::Corruption {
                            msg: format!(
                                "WAL epoch id mismatch: header.epoch_id {} != epoch_id {}",
                                header.epoch_id, epoch_id
                            ),
                            epoch_id,
                            offset: 0,
                            data: buf.to_vec(),
                        });
                    }
                    Ok(header)
                }
                Err(err) => {
                    // Haven't written the header.
                    if buf.iter().all(|v| *v == 0) {
                        return Err(Error::Eof);
                    }
                    // Header is corrupt, but the first batch header is empty which means there
                    // is no data in this WAL. Treat it like EOF and WAL writer will rewrite the
                    // header.
                    file.read_exact(&mut buf[..BATCH_HEADER_SIZE])?;
                    if buf.iter().take(BATCH_HEADER_SIZE).all(|v| *v == 0) {
                        return Err(Error::Eof);
                    }
                    // Header corruption.
                    Err(err)
                }
            };
        }
    }
    Err(Error::Eof)
}

pub(crate) struct WalWriter {
    pub(crate) dir: PathBuf,
    pub(crate) version: Version,
    pub(crate) epoch_id: u32,
    pub(crate) wal_size: usize,
    fd: Option<File>,
    buf: DmaBuffer,
    // batch_buf is unformatted data.
    batch_buf: DmaBuffer,
    compression_threshold: usize,
    // file_off is always aligned.
    pub(crate) file_off: u64,
    pub(crate) compacted_epoch: Arc<AtomicU32>,
    pub(crate) writer_type: WriterType,
    pub(crate) write_throttle_duration: Duration,
    pub(crate) epoch_rotate_len: usize,
    pub(crate) max_batch_size: usize,
}

impl WalWriter {
    pub(crate) fn new(
        dir: &Path,
        cfg: &Config,
        compacted_epoch: Arc<AtomicU32>,
        writer_type: WriterType,
    ) -> Self {
        let version = Version::V2;
        let mut buf = DmaBuffer::new(INITIAL_BUF_SIZE);
        buf.ensure_space(BATCH_HEADER_SIZE);
        // Safety: ensured enough space and `flush` will init the header.
        unsafe {
            buf.advance_mut(BATCH_HEADER_SIZE);
        }
        let wal_size = cfg.target_file_size.0 as usize;
        let compression_threshold = cfg.batch_compression_threshold.0 as usize;
        let write_throttle_duration = cfg.write_throttle_duration.0;
        let epoch_rotate_len = cfg.epoch_rotate_len;
        let max_batch_size = cfg.max_batch_size.0 as usize;
        Self {
            dir: dir.to_path_buf(),
            version,
            epoch_id: 0,
            wal_size: DmaBuffer::aligned_len(wal_size),
            fd: None,
            buf,
            batch_buf: DmaBuffer::new(INITIAL_BUF_SIZE),
            compression_threshold,
            file_off: 0,
            compacted_epoch,
            writer_type,
            write_throttle_duration,
            epoch_rotate_len,
            max_batch_size,
        }
    }

    pub(crate) fn open_file(&mut self, epoch_id: u32, file_off: u64) -> Result<()> {
        self.epoch_id = epoch_id;
        self.file_off = file_off;

        let filename = wal_file_name(&self.dir, epoch_id, self.epoch_rotate_len);
        let file = match self.writer_type {
            WriterType::Sync => open_direct_file(&filename, true)?,
            WriterType::Async => {
                if let Some(fd) = &self.fd {
                    fd.sync_all()?;
                }
                // Must not use Direct I/O for async writer.
                // Otherwise readers (`Worker` & `ObjectStorageWorker`) using buffer I/O would
                // get incomplete data.
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(filename)?
            }
            WriterType::CliMode => {
                // For cli tools like full restoration, avoid using `O_DSYNC` to improve I/O
                // performance. Data in the buffer will be flushed automatically
                // when file is dropped.
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(filename)?
            }
        };
        self.fd = Some(file);

        if file_off == 0 {
            self.write_header()?;
        } else {
            match check_wal_header(self.dir.as_path(), epoch_id, self.epoch_rotate_len) {
                Ok(_) => {}
                Err(Error::Eof) => {
                    self.file_off = 0;
                    self.write_header()?;
                }
                Err(e) => return Err(e),
            };
        }
        Ok(())
    }

    pub(crate) fn file(&self) -> &File {
        self.fd.as_ref().unwrap()
    }

    pub(crate) fn append_region_data(&mut self, peer_batch: &PeerBatch) {
        let data_len = peer_batch.encoded_len();
        ENGINE_REGION_WRITE_BATCH_SIZE_HISTOGRAM.observe(data_len as f64);
        self.batch_buf.ensure_space(data_len);
        // Safety: `data_len` is the length of data encoded by `encode_to` and
        // `ensure_space` ensures enough space.
        unsafe {
            peer_batch.encode_to(&mut self.batch_buf.chunk_mut());
            self.batch_buf.advance_mut(data_len);
        }
    }

    pub(crate) fn flush(&mut self) -> Result<(u32, u64, bool)> {
        self.compress_batch();
        let batch = self.buf.as_mut();
        let (mut batch_header, batch_payload) = batch.split_at_mut(BATCH_HEADER_SIZE);
        let checksum = crc32c::crc32c(batch_payload);
        batch_header.put_u32_le(self.epoch_id);
        batch_header.put_u32_le(checksum);
        batch_header.put_u32_le(batch_payload.len() as u32);
        self.buf.pad_to_align();
        let aligned_len = self.buf.len();
        // An empty batch header is added after each new batch to differentiate the old
        // record.
        write_eof(&mut self.buf);

        let mut rotated = false;
        // Check should_rotate or should_chunk after put this write batch to buf avoid
        // file size overflow.
        if self.should_rotate() {
            let mut total_throttle_dur = Duration::from_secs(0);
            while !self.safe_to_rotate() {
                let throttle_once_duration = Duration::from_secs(1);
                std::thread::sleep(throttle_once_duration);
                total_throttle_dur += throttle_once_duration;
                warn!("epoch {} is not safe to rotate", self.epoch_id);
            }
            if !total_throttle_dur.is_zero() {
                RFENGINE_WRITE_THROTTLE_DURATION_HISTOGRAM
                    .observe(total_throttle_dur.as_secs_f64());
            }
            self.rotate()?;
            // Writer epoch increased, also need update epoch_id in buf
            self.buf.as_mut().put_u32_le(self.epoch_id);
            rotated = true;
        }
        if let Some(duration) = self.need_throttle() {
            RFENGINE_WRITE_THROTTLE_DURATION_HISTOGRAM.observe(duration.as_secs_f64());
            std::thread::sleep(duration);
        }

        let timer = Instant::now();
        self.file().write_all_at(self.buf.as_ref(), self.file_off)?;
        let write_duration = timer.saturating_elapsed();
        Self::maybe_log_slow_write(write_duration, aligned_len);
        ENGINE_WAL_WRITE_DURATION_HISTOGRAM.observe(write_duration.as_secs_f64());
        self.file_off += aligned_len as u64;
        self.buf.truncate(BATCH_HEADER_SIZE);

        Ok((self.epoch_id, self.file_off, rotated))
    }

    fn maybe_log_slow_write(write_duration: Duration, aligned_len: usize) {
        if write_duration > Duration::from_millis(40) && aligned_len < 64 * 1024 {
            info!(
                "wal write takes too long {:?}, size: {}",
                write_duration, aligned_len
            );
        }
    }

    pub(crate) fn write_batch(&mut self, wb: &[PeerBatch]) -> Result<(u32, u64, bool)> {
        for peer_batch in wb {
            self.append_region_data(peer_batch);
        }
        if self.batch_buf.len() > self.max_batch_size {
            // The batch is unexpectedly too large, we reject the write, find the culprit
            // region and later panic can blacklist it.
            tikv_util::set_current_region(self.find_largest_region(wb));
            error!(
                "write batch size {} exceed max batch size {}",
                self.batch_buf.len(),
                self.max_batch_size
            );
            return Err(Error::MaxBatchSizeExceeded);
        }
        self.flush()
    }

    fn find_largest_region(&self, wb: &[PeerBatch]) -> u64 {
        wb.iter()
            .max_by_key(|batch| batch.encoded_len())
            .map(|batch| batch.region_id)
            .unwrap_or_default()
    }

    fn compress_batch(&mut self) {
        unsafe {
            let compression = self.batch_buf.len() >= self.compression_threshold;
            let compression_type = u32::from(compression);
            self.buf.ensure_space(4);
            self.buf.chunk_mut().put_u32_le(compression_type);
            self.buf.advance_mut(4);
            if compression {
                self.buf.ensure_space(4);
                self.buf.chunk_mut().put_u32_le(self.batch_buf.len() as u32);
                self.buf.advance_mut(4);
                let compress_bound = lz4::liblz4::LZ4_compressBound(self.batch_buf.len() as i32);
                self.buf.ensure_space(compress_bound as usize);
                let src = self.batch_buf.as_mut();
                let dst = self.buf.chunk_mut();
                let size = lz4::liblz4::LZ4_compress_default(
                    src.as_ptr() as *const libc::c_char,
                    dst.as_mut_ptr() as *mut libc::c_char,
                    src.len() as i32,
                    compress_bound,
                ) as usize;
                self.buf.advance_mut(size);
            } else {
                self.buf.ensure_space(self.batch_buf.len());
                self.buf.chunk_mut().put_slice(self.batch_buf.as_ref());
                self.buf.advance_mut(self.batch_buf.len());
            }
            self.batch_buf.truncate(0);
        }
    }

    fn should_rotate(&self) -> bool {
        let current_size = self.buf.len() + self.file_off as usize;
        current_size > self.wal_size && self.writer_type != WriterType::Async
    }

    // If the current epoch id is 5, the rotated epoch id is 6, it would overwrite
    // epoch 2 wal, so we need to make sure epoch 2 is compacted.
    fn safe_to_rotate(&self) -> bool {
        let compacted_epoch = self.compacted_epoch.load(Ordering::SeqCst);
        compacted_epoch + self.epoch_rotate_len as u32 > self.epoch_id
    }

    // When WAL compact is slow, we should slow down to make the compaction catch
    // up.
    // If the current epoch id is 5, the compacted_epoch can be 1, 2, 3, 4,
    // we sleep for write_throttle_duration when compacted_epoch is 2,
    // sleep for write_throttle_duration * 4 when compacted_epoch is 1.
    fn need_throttle(&self) -> Option<Duration> {
        let compacted_epoch = self.compacted_epoch.load(Ordering::SeqCst);
        if self.epoch_id < self.epoch_rotate_len as u32 {
            return None;
        }
        match (compacted_epoch + self.epoch_rotate_len as u32 - 1).cmp(&self.epoch_id) {
            cmp::Ordering::Less => Some(self.write_throttle_duration * 4),
            cmp::Ordering::Equal => Some(self.write_throttle_duration),
            cmp::Ordering::Greater => None,
        }
    }

    pub(crate) fn rotate(&mut self) -> Result<()> {
        let timer = Instant::now_coarse();
        self.open_file(self.epoch_id + 1, 0)?;
        ENGINE_ROTATE_DURATION_HISTOGRAM.observe(timer.saturating_elapsed_secs());
        Ok(())
    }

    fn write_header(&mut self) -> Result<()> {
        let mut buf = DmaBuffer::new(WalHeader::len());
        unsafe {
            let header = WalHeader::new(self.version, self.epoch_id);
            header.encode_to(buf.chunk_mut());
            buf.advance_mut(WalHeader::len());
            buf.pad_to_align();
        }
        self.file_off = buf.len() as u64;
        write_eof(&mut buf);
        self.file().write_all_at(buf.as_ref(), 0)?;
        let wal_size = self.wal_size as u64;
        self.file().set_len(wal_size)?;
        Ok(())
    }
}

pub(crate) fn write_eof(buf: &mut DmaBuffer) {
    buf.ensure_space(BATCH_HEADER_SIZE);
    unsafe {
        let chunk = buf.chunk_mut();
        chunk[..BATCH_HEADER_SIZE].fill(0);
        buf.advance_mut(BATCH_HEADER_SIZE);
    }
    buf.pad_to_align();
}

pub(crate) enum WalWriterExt {
    SingleWriter(WalWriter),
    DoubleWriter(DoubleWriter),
}

impl WalWriterExt {
    pub(crate) fn write_batch(&mut self, wb: Arc<Vec<PeerBatch>>) -> Result<(u32, u64, bool)> {
        match self {
            WalWriterExt::SingleWriter(writer) => writer.write_batch(wb.as_slice()),
            WalWriterExt::DoubleWriter(double_writer) => double_writer.write_batch(wb),
        }
    }

    pub(crate) fn get_file_off(&self) -> u64 {
        match self {
            WalWriterExt::SingleWriter(writer) => writer.file_off,
            WalWriterExt::DoubleWriter(double_writer) => double_writer.file_off,
        }
    }

    pub(crate) fn get_epoch_id(&self) -> u32 {
        match self {
            WalWriterExt::SingleWriter(writer) => writer.epoch_id,
            WalWriterExt::DoubleWriter(double_writer) => double_writer.epoch_id,
        }
    }

    pub(crate) fn open_file(&mut self, epoch_id: u32, file_off: u64) -> Result<()> {
        match self {
            WalWriterExt::SingleWriter(writer) => writer.open_file(epoch_id, file_off),
            WalWriterExt::DoubleWriter(writer) => writer.open_file(epoch_id, file_off),
        }
    }
}

fn load_epoch_offset(
    wal_dir: &Path,
    manifest_epoch: u32,
    epoch_rotate_len: usize,
) -> Result<(u32, u64)> {
    let mut epoch_id = manifest_epoch + 1;
    if !wal_exists(wal_dir, epoch_id, epoch_rotate_len) {
        // may fall behind too much or newly created wal dir.
        return Ok((epoch_id, 0));
    }
    while wal_exists(wal_dir, epoch_id + 1, epoch_rotate_len) {
        epoch_id += 1;
    }
    let mut iter = WalIterator::new(wal_dir, epoch_id, epoch_rotate_len)?;
    iter.iterate_batch(|_, _| {})?;
    Ok((epoch_id, iter.offset))
}

#[derive(Default)]
pub(crate) struct DoubleWriter {
    pub(crate) senders: Vec<Sender<DoubleWriterMessage>>,
    pub(crate) handles: Vec<JoinHandle<()>>,
    pub(crate) epoch_id: u32,
    pub(crate) file_off: u64,
    pub(crate) total_size: Arc<AtomicUsize>,
}

impl DoubleWriter {
    pub(crate) fn new(
        primary_writer: WalWriter,
        secondary_writer: WalWriter,
        manifest_epoch: u32,
        unhealthy_size: usize,
    ) -> Result<Self> {
        Self::sync_writer_files(
            &primary_writer.dir,
            &secondary_writer.dir,
            manifest_epoch,
            primary_writer.epoch_rotate_len,
        )?;
        let total_size = Arc::new(AtomicUsize::new(0));
        let (primary_sender, primary_handle) =
            DoubleWriterWorker::start(primary_writer, unhealthy_size, total_size.clone());
        let (secondary_sender, secondary_handle) =
            DoubleWriterWorker::start(secondary_writer, unhealthy_size, total_size.clone());
        RFENGINE_DOUBLE_WRITE_HEALTHY_GAUGE.set(1);
        Ok(Self {
            senders: vec![primary_sender, secondary_sender],
            handles: vec![primary_handle, secondary_handle],
            epoch_id: 0, // will be set on open_file.
            file_off: 0, // will be set on open_file.
            total_size,
        })
    }

    pub(crate) fn sync_writer_files(
        primary_dir: &PathBuf,
        secondary_dir: &PathBuf,
        manifest_epoch: u32,
        epoch_rotate_len: usize,
    ) -> Result<()> {
        let (primary_epoch_id, primary_file_off) =
            load_epoch_offset(primary_dir.as_path(), manifest_epoch, epoch_rotate_len)?;
        let (secondary_epoch_id, secondary_file_off) =
            load_epoch_offset(secondary_dir.as_path(), manifest_epoch, epoch_rotate_len)?;
        let mut faster_dir = primary_dir;
        let mut faster_epoch = primary_epoch_id;
        let mut faster_file_off = primary_file_off;
        let mut slower_dir = secondary_dir;
        let mut slower_epoch = secondary_epoch_id;
        let mut slower_file_off = secondary_file_off;
        if (primary_epoch_id, primary_file_off) < (secondary_epoch_id, secondary_file_off) {
            faster_dir = secondary_dir;
            faster_epoch = secondary_epoch_id;
            faster_file_off = secondary_file_off;
            slower_dir = primary_dir;
            slower_epoch = primary_epoch_id;
            slower_file_off = primary_file_off;
        }
        let fast_dir_str = faster_dir.to_string_lossy();
        let slow_dir_str = slower_dir.to_string_lossy();
        if faster_epoch != slower_epoch {
            // The slower writer maybe newly created, so we need to copy all files.
            let start_epoch = (manifest_epoch + 1).max(slower_epoch);
            for epoch_id in start_epoch..=faster_epoch {
                let faster_file_path = wal_file_name(faster_dir, epoch_id, epoch_rotate_len);
                let slower_file_path = wal_file_name(slower_dir, epoch_id, epoch_rotate_len);
                info!(
                    "copy file at epoch {} from {} to {}",
                    epoch_id, fast_dir_str, slow_dir_str,
                );
                file_system::copy_and_sync(faster_file_path.as_path(), slower_file_path.as_path())?;
            }
            file_system::sync_dir(slower_dir.as_path())?;
        } else if faster_file_off != slower_file_off {
            // Only need to copy the delta.
            let faster_file_path = wal_file_name(faster_dir, faster_epoch, epoch_rotate_len);
            let faster_file = File::open(faster_file_path)?;
            let delta_size = (faster_file_off - slower_file_off) + BATCH_HEADER_SIZE as u64;
            let mut delta_buf = vec![0u8; delta_size as usize];
            faster_file.read_exact_at(&mut delta_buf, slower_file_off)?;
            let slower_file_path = wal_file_name(slower_dir, faster_epoch, epoch_rotate_len);
            info!(
                "sync wal files at epoch {} from {} file_off {} to {} file_off {}",
                faster_epoch, fast_dir_str, faster_file_off, slow_dir_str, slower_file_off
            );
            let slower_file = File::options()
                .create(true)
                .truncate(false)
                .write(true)
                .open(slower_file_path)?;
            slower_file.write_all_at(&delta_buf, slower_file_off)?;
            slower_file.sync_data()?;
        }
        Ok(())
    }

    pub(crate) fn write_batch(&mut self, wb: Arc<Vec<PeerBatch>>) -> Result<(u32, u64, bool)> {
        let (tx, rx) = tikv_util::mpsc::bounded(2);
        self.total_size
            .fetch_add(write_batch_size(&wb), Ordering::SeqCst);
        for sender in &self.senders {
            sender
                .send(DoubleWriterMessage::Write {
                    wb: wb.clone(),
                    res_tx: tx.clone(),
                })
                .unwrap();
        }
        let (epoch, offset, rotated) = rx.recv().unwrap()?;
        self.epoch_id = epoch;
        self.file_off = offset;
        Ok((epoch, offset, rotated))
    }

    pub(crate) fn open_file(&mut self, epoch_id: u32, file_off: u64) -> Result<()> {
        let (tx, rx) = tikv_util::mpsc::bounded(2);
        for sender in &self.senders {
            let msg = DoubleWriterMessage::OpenFile {
                epoch_id,
                file_off,
                res_tx: tx.clone(),
            };
            sender.send(msg).unwrap();
        }
        rx.recv().unwrap()?;
        rx.recv().unwrap()?;
        self.epoch_id = epoch_id;
        self.file_off = file_off;
        Ok(())
    }
}

impl Drop for DoubleWriter {
    fn drop(&mut self) {
        for sender in &self.senders {
            sender.send(DoubleWriterMessage::Stop).ok();
        }
        for handle in mem::take(&mut self.handles) {
            let _ = handle.join();
        }
    }
}

pub(crate) struct DoubleWriterWorker {
    writer: WalWriter,
    rx: Receiver<DoubleWriterMessage>,
    healthy: bool,
    unhealthy_size: usize,
    total_size: Arc<AtomicUsize>,
    handled_size: usize,
}

pub(crate) enum DoubleWriterMessage {
    Write {
        wb: Arc<Vec<PeerBatch>>,
        res_tx: Sender<Result<(u32, u64, bool)>>,
    },
    OpenFile {
        epoch_id: u32,
        file_off: u64,
        res_tx: Sender<Result<()>>,
    },
    Stop,
}

impl DoubleWriterWorker {
    pub(crate) fn start(
        writer: WalWriter,
        unhealthy_size: usize,
        total_size: Arc<AtomicUsize>,
    ) -> (Sender<DoubleWriterMessage>, JoinHandle<()>) {
        let (tx, rx) = tikv_util::mpsc::unbounded();
        let handle = std::thread::spawn(move || {
            let mut worker = Self {
                writer,
                rx,
                healthy: true,
                unhealthy_size,
                total_size,
                handled_size: 0,
            };
            worker.run();
        });
        (tx, handle)
    }

    pub(crate) fn run(&mut self) {
        while let Ok(msg) = self.rx.recv() {
            match msg {
                DoubleWriterMessage::Write { wb, res_tx } => {
                    if !self.healthy {
                        continue;
                    }
                    let fall_behind_size =
                        self.total_size.load(Ordering::Relaxed) - self.handled_size;
                    if fall_behind_size > self.unhealthy_size {
                        // If the writer fall behind too much, we stop writing.
                        self.healthy = false;
                        let dir_str = self.writer.dir.to_string_lossy();
                        tikv_util::error!(
                            "double writer {} is unhealthy, too many pending messages",
                            dir_str
                        );
                        RFENGINE_DOUBLE_WRITE_HEALTHY_GAUGE.set(0);
                        continue;
                    }
                    self.handled_size += write_batch_size(&wb);
                    let _ = res_tx.send(self.writer.write_batch(&wb));
                }
                DoubleWriterMessage::OpenFile {
                    epoch_id,
                    file_off,
                    res_tx,
                } => {
                    let result = self.writer.open_file(epoch_id, file_off);
                    let _ = res_tx.send(result);
                }
                DoubleWriterMessage::Stop => {
                    return;
                }
            }
        }
    }
}

fn write_batch_size(wb: &[PeerBatch]) -> usize {
    wb.iter().map(|b| b.encoded_len()).sum()
}

#[cfg(test)]
mod tests {
    use std::{os::unix::fs::FileExt, sync::atomic::Ordering::SeqCst};

    use bytes::Buf;
    use rand::Rng;
    use tikv_util::{config::ReadableSize, info};

    use super::DmaBuffer;
    use crate::{
        BATCH_HEADER_SIZE, Error, RfEngine, WalHeader, WriteBatch,
        compact_worker::wal_file_name,
        config::Config,
        iterator::WalIterator,
        test_util::{
            get_epoch_file_off, init_logger, make_state_kv, prepare_rfengine,
            prepare_rfengine_with_idx,
        },
        writer::Version::V2,
    };

    #[test]
    fn test_dma_buffer() {
        let mut buf = DmaBuffer::new(4095);
        assert_eq!(buf.layout.size(), DmaBuffer::aligned_len(4095));
        let addr = buf.data.as_ptr() as usize;
        assert_eq!(addr, DmaBuffer::aligned_len(addr));

        let data = b"data";
        for i in 1..=1025 {
            buf.ensure_space(data.len());
            let chunk = unsafe { buf.chunk_mut() };
            chunk[..data.len()].copy_from_slice(data);
            unsafe { buf.advance_mut(data.len()) };
            assert_eq!(buf.len(), data.len() * i);
            assert_eq!(buf.as_ref(), data.repeat(i));
            assert_eq!(buf.as_mut(), data.repeat(i));
        }
        assert_eq!(buf.layout.size(), 8192);
        let addr = buf.data.as_ptr() as usize;
        assert_eq!(addr, DmaBuffer::aligned_len(addr));
    }

    #[test]
    fn test_wal_header() {
        let wal_header = WalHeader::new(V2, 1);
        let mut buf = [0_u8; WalHeader::len()];
        wal_header.encode_to(buf.as_mut_slice());
        assert_eq!(WalHeader::decode(buf.as_slice()).unwrap(), wal_header);
    }

    #[test]
    fn test_double_writer() {
        init_logger();
        let tmp_dir = tempfile::tempdir().unwrap();
        let wal_size = 512 * 1024_usize;
        let dir_path = tmp_dir.path();
        let sync_wal_path = dir_path.join("wal-sync");
        let wal_secondary_path = dir_path.join("wal-secondary");
        let mut cfg = Config::new(wal_size);
        cfg.wal_sync_dir = sync_wal_path.to_str().unwrap().to_owned();
        cfg.wal_secondary_dir = wal_secondary_path.to_str().unwrap().to_owned();
        let engine = RfEngine::open(dir_path, &cfg, None, None).unwrap();
        prepare_rfengine(&engine);
        // Stop the worker to make compaction fall behind, to cover the case that two
        // writers on different epoch.
        engine.stop_worker(true);
        let mut rnd = rand::thread_rng();
        prepare_rfengine_with_idx(&engine, 1051, 1051 + rnd.gen_range(0..300));
        let (epoch, file_off) = get_epoch_file_off(&engine);
        let manifest_epoch = engine.compacted_epoch.load(SeqCst);
        info!(
            "manifest_epoch: {}, current_epoch: {}, file_off: {}",
            manifest_epoch, epoch, file_off
        );
        let engine_stats = engine.get_engine_stats();
        drop(engine);
        for _ in 0..10 {
            // randomly truncate a wal file to simulate a slow wal writer.
            let truncate_path = if rnd.gen_bool(0.5) {
                &sync_wal_path
            } else {
                &wal_secondary_path
            };
            let truncate_epoch_id = rnd.gen_range((manifest_epoch + 1)..=epoch);
            let wal_file = wal_file_name(
                truncate_path.as_path(),
                truncate_epoch_id,
                cfg.epoch_rotate_len,
            );
            assert!(wal_file.exists());
            let mut wal_iter =
                WalIterator::new(truncate_path, truncate_epoch_id, cfg.epoch_rotate_len).unwrap();
            let mut file_offs = vec![];
            wal_iter
                .iterate_batch(|_, file_off| {
                    file_offs.push(file_off);
                })
                .unwrap();
            if file_offs.is_empty() {
                continue;
            }
            let truncate_file_off = file_offs[rnd.gen_range(0..file_offs.len())];
            info!(
                "truncate wal file epoch {}, file_off: {}",
                truncate_epoch_id, truncate_file_off
            );
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&wal_file)
                .unwrap();
            let eof_data = vec![0u8; BATCH_HEADER_SIZE];
            file.write_all_at(&eof_data, truncate_file_off).unwrap();
            // We also need to reset the next file header to simulate a slow wal writer.
            // Because we fast check the next file header to determine if the current file
            // is valid.
            let next_wal_file_path = wal_file_name(
                truncate_path.as_path(),
                truncate_epoch_id + 1,
                cfg.epoch_rotate_len,
            );
            let next_wal_file = std::fs::OpenOptions::new()
                .write(true)
                .open(&next_wal_file_path)
                .unwrap();
            next_wal_file.write_all_at(&eof_data, 0).unwrap();
            // Set to cli_mode to avoid compaction.
            cfg.cli_mode = true;
            cfg.disable_compaction = true;
            let engine = RfEngine::open(dir_path, &cfg, None, None).unwrap();
            let (new_epoch, new_file_off) = get_epoch_file_off(&engine);
            assert_eq!(new_epoch, epoch);
            assert_eq!(new_file_off, file_off);
            let new_stats = engine.get_engine_stats();
            assert_eq!(new_stats.total_mem_entries, engine_stats.total_mem_entries);
        }
    }

    #[test]
    fn test_exceed_max_write_batch() {
        init_logger();
        let tmp_dir = tempfile::tempdir().unwrap();
        let wal_size = 512 * 1024_usize;
        let dir_path = tmp_dir.path();
        let sync_wal_path = dir_path.join("wal-sync");
        let mut cfg = Config::new(wal_size);
        cfg.wal_sync_dir = sync_wal_path.to_str().unwrap().to_owned();
        cfg.max_batch_size = ReadableSize(256);
        let engine = RfEngine::open(dir_path, &cfg, None, None).unwrap();

        let mut wb = WriteBatch::new();
        let largest_region = 5;
        for peer_id in 1..=10_u64 {
            let region_id = peer_id + 1;
            if region_id == largest_region {
                let (key, val) = make_state_kv(4, 1);
                wb.set_state(peer_id, region_id, 1, key.chunk(), val.chunk());
            }
            let (key, val) = make_state_kv(3, 1);
            wb.set_state(peer_id, region_id, 1, key.chunk(), val.chunk());
        }
        let res = engine.persist(wb);
        let err = res.unwrap_err();
        assert!(matches!(err, Error::MaxBatchSizeExceeded));
        assert_eq!(tikv_util::get_current_region(), largest_region);
    }
}

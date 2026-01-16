// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashSet,
    convert::TryFrom,
    fmt, fs,
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use log_wrappers::Value as LogValue;
use schema::schema::StorageClass;

use crate::{
    FileMeta, IoContext, dfs,
    dfs::{Dfs, FileType},
    ia::{
        manager::{IaManager, ReadAt},
        types::{FileSegmentIdent, FileSegmentPosition, TABLE_META_LOCAL_FILE_SUFFIX},
    },
    metrics::{ENGINE_IA_SYNC_READ_COUNTER, PREPARE_COUNTER_VEC},
    new_blob_filename, new_columnar_filename, new_fts_dedicated_filename, new_fts_packed_filename,
    new_sst_filename, new_vector_index_filename,
    table::{
        Error, Result,
        blobtable::{self, blobtable::BlobTable},
        columnar::{ColumnarFileFooter, TableMeta, TableOffsets},
        file::{File, FileMmapGuard, InMemFile, MmapData},
        search, sstable,
        sstable::{Index, SsTable},
        tiny_meta::{SstTinyMeta, TypedTinyMeta},
        vector_index::VectorIndexFileFooter,
    },
};

#[derive(Clone)]
pub struct IaFile {
    pub(crate) id: u64,
    pub(crate) size: u64,
    pub(crate) ftype: FileType,
    pub(crate) table_meta_off: u64,
    pub(crate) segment_offsets: Vec<u64>,
    pub(crate) table_meta_file: Arc<dyn File>,
    pub(crate) mgr: IaManager,
}

impl fmt::Debug for IaFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IaFile")
            .field("id", &self.id)
            .field("size", &self.size)
            .field("ftype", &self.ftype)
            .field("table_meta_off", &self.table_meta_off)
            .field("segment_offsets", &self.segment_offsets)
            .finish()
    }
}

impl IaFile {
    pub fn open(
        id: u64,
        fm: &FileMeta,
        table_meta_file: Arc<dyn File>,
        mgr: IaManager,
    ) -> Result<Self> {
        Self::open_with_tiny_meta(id, fm, table_meta_file, mgr, &TypedTinyMeta::default())
    }

    pub fn open_with_tiny_meta(
        id: u64,
        fm: &FileMeta,
        table_meta_file: Arc<dyn File>,
        mgr: IaManager,
        tiny_meta: &TypedTinyMeta,
    ) -> Result<Self> {
        match fm.file_type {
            FileType::Sst => Self::open_for_sst(id, table_meta_file, mgr, tiny_meta.as_sst()),
            FileType::Columnar => Self::open_for_columnar(id, table_meta_file, mgr),
            FileType::VectorIndex => Self::open_for_vector(id, fm, table_meta_file, mgr),
            FileType::FtsPackedFile => Self::open_for_fts_packed(id, fm, table_meta_file, mgr),
            FileType::FtsDedicatedFile => {
                Self::open_for_fts_dedicated(id, fm, table_meta_file, mgr)
            }
            FileType::Blob => Self::open_for_blob(id, table_meta_file, mgr),
            _ => Err(Error::IaMgr(format!(
                "{id} open: file type not supported: {:?}",
                fm.file_type
            ))),
        }
    }

    pub(crate) fn open_for_sst(
        id: u64,
        table_meta_file: Arc<dyn File>,
        mgr: IaManager,
        tiny_meta_opt: Option<&SstTinyMeta>,
    ) -> Result<Self> {
        let footer = match tiny_meta_opt {
            Some(tiny_meta) => tiny_meta.footer,
            None => {
                let footer_data = table_meta_file.read_footer(SsTable::footer_size())?;
                let mut footer = sstable::Footer::default();
                footer.unmarshal(&footer_data);
                if !footer.is_match() {
                    error!("{} open for sst: footer not match", id; "footer" => LogValue::value(&footer_data));
                    if let Some(path) = table_meta_file.path() {
                        if let Err(err) = std::fs::remove_file(path) {
                            warn!("{} open for sst: remove interrupted meta file: failed", id; "err" => ?err);
                        }
                    }
                    return Err(Error::IaMgr(
                        format!("{id} open for sst: footer not match",),
                    ));
                }
                footer
            }
        };

        let meta_size = table_meta_file.size();
        let table_meta_off = footer.meta_offset() as u64;
        let mut f = Self {
            id,
            size: table_meta_off + meta_size,
            ftype: FileType::Sst,
            table_meta_off,
            segment_offsets: vec![],
            table_meta_file,
            mgr,
        };

        match tiny_meta_opt.and_then(|tiny_meta| tiny_meta.segment_offsets.as_ref()) {
            Some(segment_offsets) if segment_offsets.len() >= 2 => {
                // Require at least two offsets to avoid out-of-bounds in align_to_segment.
                PREPARE_COUNTER_VEC.tiny_meta_segment_offsets_hit.inc();
                f.segment_offsets = segment_offsets
                    .iter()
                    .map(|&offset| u64::from(offset))
                    .collect();
            }
            Some(segment_offsets) => {
                warn!(
                    "{} open for sst: invalid tiny meta segment offsets: {:?}",
                    id, segment_offsets
                );
                debug_assert!(false);
                f.generate_segment_offsets_for_sst(&footer)?;
            }
            None => {
                f.generate_segment_offsets_for_sst(&footer)?;
            }
        }

        debug!("{} open for sst: {:?}", id, f);
        Ok(f)
    }

    fn generate_segment_offsets_for_sst(&mut self, footer: &sstable::Footer) -> Result<()> {
        let segment_size = self.mgr.segment_size();
        let mut builder = SegmentOffsetsBuilder::new(segment_size as u64);

        let idx_data = self.read_table_meta(footer.index_offset as u64, footer.index_len())?;
        sstable::validate_checksum_with_fix(
            &idx_data,
            footer,
            self.table_meta_file.as_ref(),
            None,
        )?;
        let idx = sstable::Index::new(idx_data)?;
        builder.push_from_sstable_idx(&idx);
        builder.push_boundary(footer.data_len() as u64);

        if footer.old_data_len() > 0 {
            debug_assert!(footer.old_index_len() > 0);
            let old_idx_data =
                self.read_table_meta(footer.old_index_offset as u64, footer.old_index_len())?;
            sstable::validate_checksum_with_fix(
                &old_idx_data,
                footer,
                self.table_meta_file.as_ref(),
                None,
            )?;
            let old_idx = sstable::Index::new(old_idx_data)?;
            builder.push_from_sstable_idx(&old_idx);
            builder.push_boundary((footer.data_len() + footer.old_data_len()) as u64);
        }

        self.segment_offsets = builder.finish();
        debug!("generate segment offsets: {:?}", self.segment_offsets; "file_id" => self.id);
        Ok(())
    }

    fn open_for_columnar(id: u64, table_meta_file: Arc<dyn File>, mgr: IaManager) -> Result<Self> {
        let footer_data = table_meta_file.read_footer(ColumnarFileFooter::compute_size())?;
        let footer = ColumnarFileFooter::parse(&footer_data);
        let meta_size = table_meta_file.size();
        let table_offsets_size = TableOffsets::compute_size(footer.number_tables as usize) as u64;
        let table_offsets_offset =
            meta_size - ColumnarFileFooter::compute_size() as u64 - table_offsets_size;
        let table_offsets_data =
            table_meta_file.read_table_meta(table_offsets_offset, table_offsets_size as usize)?;
        let table_offsets = TableOffsets::parse(&table_offsets_data, footer.number_tables);
        let table_meta_off = table_offsets.index_offset() as u64;
        let segment_size = mgr.segment_size();
        let mut f = Self {
            id,
            size: table_meta_off + meta_size,
            ftype: FileType::Columnar,
            table_meta_off,
            segment_offsets: vec![],
            table_meta_file: table_meta_file.clone(),
            mgr,
        };

        // Generate segment offsets.
        {
            let mut builder = SegmentOffsetsBuilder::new(segment_size as u64);
            for i in 0..footer.number_tables {
                let (idx_start, idx_end) = table_offsets.get_index_range(i as usize);
                let table_index_data = f.read_table_meta(
                    table_meta_off + idx_start as u64,
                    (idx_end - idx_start) as usize,
                )?;
                let table_meta =
                    TableMeta::parse(table_offsets.table_ids[i as usize], &table_index_data);
                builder.push_from_columnar_table_meta(&table_meta);
                builder.push_boundary(table_meta_off + idx_start as u64);
            }
            f.segment_offsets = builder.finish();
        }

        debug!("{} ia open for columnar: {:?}", id, f);
        Ok(f)
    }

    // table_meta_file maybe the whole vector index file if table_offset is 0.
    fn open_for_vector(
        id: u64,
        fm: &FileMeta,
        table_meta_file: Arc<dyn File>,
        mgr: IaManager,
    ) -> Result<Self> {
        let file_len = table_meta_file.size();
        let footer_size = VectorIndexFileFooter::size();
        let footer_data = table_meta_file.read_footer(footer_size)?;
        let mut footer = VectorIndexFileFooter::default();
        footer.unmarshal(&footer_data);
        let meta_size = footer.props_size() + footer_size;
        let (file_size, meta_offset) = if fm.table_meta_off == 0 {
            // If the table_meta_off is 0, it means the vector index file is built by old
            // version, and the table_meta_file is the whole file.
            let meta_offset = file_len - meta_size as u64;
            (file_len, meta_offset)
        } else {
            (
                fm.table_meta_off as u64 + meta_size as u64,
                fm.table_meta_off as u64,
            )
        };
        // If the table_meta_file is the whole file, we need to truncate it from the
        // meta_offset to the end.
        // NOTE: If all the tikv-server & tikv-worker are upgraded to the new version,
        // we can remove this logic.
        let table_meta_file = if table_meta_file.size() > meta_size as u64 {
            let truncated_meta_data = table_meta_file.read_table_meta(meta_offset, meta_size)?;
            let truncated_meta_file: Arc<dyn File> =
                Arc::new(InMemFile::new(id, truncated_meta_data));
            truncated_meta_file
        } else {
            table_meta_file
        };
        let f = Self {
            id,
            size: file_size,
            ftype: FileType::VectorIndex,
            table_meta_off: meta_offset,
            segment_offsets: vec![meta_offset],
            table_meta_file,
            mgr,
        };

        debug!("{} ia open for vector: {:?}", id, f);
        Ok(f)
    }

    fn open_for_fts_packed(
        id: u64,
        _fm: &FileMeta,
        table_meta_file: Arc<dyn File>,
        mgr: IaManager,
    ) -> Result<Self> {
        use crate::table::fts::{PackedFile, PackedFileFooter};
        let footer_size = PackedFileFooter::footer_size();
        let footer_data = table_meta_file.read_footer(footer_size)?;
        let footer = PackedFileFooter::unmarshal(&footer_data)
            .map_err(|e| Error::IaMgr(format!("{id} failed to unmarshal footer: {}", e)))?;

        let table_meta_off = footer.metadata_offset();
        let meta_size = table_meta_file.size();
        let total_file_size = table_meta_off + meta_size;

        let mut f = Self {
            id,
            size: total_file_size,
            ftype: FileType::FtsPackedFile,
            table_meta_off,
            segment_offsets: vec![],
            table_meta_file: table_meta_file.clone(),
            mgr,
        };

        // A correct table_meta_off must be set before feed into PackedFile, otherwise
        // PackedFile cannot read the meta from the offset correctly.

        let segments = PackedFile::generate_ia_segment_boundaries(&f, f.mgr.segment_size() as u64)
            .map_err(|e| {
                Error::IaMgr(format!(
                    "{} failed to generate ia segment boundaries: {}",
                    id, e
                ))
            })?;
        f.segment_offsets = segments;

        debug!("{} ia open for fts packed: {:?}", id, f);
        Ok(f)
    }

    fn open_for_fts_dedicated(
        id: u64,
        _fm: &FileMeta,
        table_meta_file: Arc<dyn File>,
        mgr: IaManager,
    ) -> Result<Self> {
        use crate::table::fts::{DedicatedFile, DedicatedFileFooter};
        let footer_size = DedicatedFileFooter::footer_size();
        let footer_data = table_meta_file.read_footer(footer_size)?;
        let footer = DedicatedFileFooter::unmarshal(&footer_data)
            .map_err(|e| Error::IaMgr(format!("{id} failed to unmarshal footer: {}", e)))?;

        let table_meta_off = footer.metadata_offset();
        let meta_size = table_meta_file.size();
        let total_file_size = table_meta_off + meta_size;

        let mut f = Self {
            id,
            size: total_file_size,
            ftype: FileType::FtsDedicatedFile,
            table_meta_off,
            segment_offsets: vec![],
            table_meta_file: table_meta_file.clone(),
            mgr,
        };

        // A correct table_meta_off must be set before feed into DedicatedFile,
        // otherwise DedicatedFile cannot read the meta from the offset
        // correctly.

        let segments =
            DedicatedFile::generate_ia_segment_boundaries(&f, f.mgr.segment_size() as u64)
                .map_err(|e| {
                    Error::IaMgr(format!(
                        "{} failed to generate ia segment boundaries: {}",
                        id, e
                    ))
                })?;
        f.segment_offsets = segments;

        debug!("{} ia open for fts dedicated: {:?}", id, f);
        Ok(f)
    }

    fn open_for_blob(id: u64, table_meta_file: Arc<dyn File>, mgr: IaManager) -> Result<Self> {
        let footer_data = table_meta_file.read_footer(BlobTable::footer_size())?;
        let mut footer = blobtable::builder::BlobFooter::default();
        footer.unmarshal(&footer_data);
        if !footer.is_match() {
            return Err(Error::IaMgr(format!(
                "{id} open for blob: footer not match"
            )));
        }

        let meta_size = table_meta_file.size();
        let table_meta_off = footer.index_offset as u64;
        let segment_size = mgr.segment_size() as u64;
        let mut f = Self {
            id,
            size: table_meta_off + meta_size,
            ftype: FileType::Blob,
            table_meta_off,
            segment_offsets: vec![],
            table_meta_file: table_meta_file.clone(),
            mgr,
        };

        let table_offsets_data = f.read_table_meta(table_meta_off, footer.index_len())?;
        let mut builder = SegmentOffsetsBuilder::new(segment_size);
        let index = Index::new_for_blob(table_offsets_data)?;
        for pos in 0..index.num_blocks() {
            builder.push_block_off(index.get_block_addr_for_blob(pos) as u64);
        }
        builder.push_boundary(table_meta_off);
        f.segment_offsets = builder.finish();

        Ok(f)
    }

    async fn read_range(&self, mut read_at: ReadAt<'_>) -> Result<()> {
        let (start_off, end_off) = (read_at.start_off(), read_at.end_off());

        // Read across meta should not happen.
        // But following process can still handle the read correctly.
        debug_assert!(
            end_off <= self.table_meta_off,
            "{} read across meta, end {}, file {:?}",
            self.id,
            end_off,
            self
        );

        let ident = self.align_to_segment(start_off, end_off)?;
        debug!("{} read range", self.id; "start" => start_off, "end" => end_off, "ident" => %ident);
        self.mgr
            .read_segment(ident, self.ftype, None, None, &mut read_at, None)
            .await
    }

    fn align_to_segment(&self, start_off: u64, end_off: u64) -> Result<FileSegmentIdent> {
        align_to_segment(self.id, &self.segment_offsets, start_off, end_off)
    }

    /// Used for remote-cop scene.
    pub async fn prepare_table_meta(
        file_id: u64,
        ftype: FileType,
        table_meta_off: u64,
        data_dir: &Path,
        opts: &dfs::Options,
        ia_mgr: &IaManager,
        dfs: Option<&dyn Dfs>,
    ) -> Result<Bytes> {
        let local_path = table_meta_file_local_path(file_id, ftype, data_dir);
        let local_path_cloned = local_path.clone();
        let runtime = ia_mgr.get_dfs().get_runtime();
        let read_result = runtime.spawn_blocking(move || fs::read(local_path_cloned));
        let ia_dfs = ia_mgr.get_dfs();
        let dfs = dfs.unwrap_or(ia_dfs);
        let table_meta_data = match read_result.await.unwrap() {
            Ok(bytes) => {
                let should_set_mtime = ia_mgr.access_table_meta(file_id);
                if should_set_mtime {
                    if let Err(err) =
                        filetime::set_file_mtime(&local_path, filetime::FileTime::now())
                    {
                        debug_assert!(
                            err.kind() == ErrorKind::NotFound,
                            "{} set mtime failed: {:?}",
                            file_id,
                            err
                        );
                        warn!("{} prepare meta: set file mtime failed", file_id; "err" => ?err);
                    }
                }
                Bytes::from(bytes)
            }
            Err(err) if err.kind() == ErrorKind::NotFound => {
                let opts = opts.with_type(ftype).with_start_off(table_meta_off);
                let bytes = dfs.read_file(file_id, opts).await.map_err(|err| {
                    Error::IaMgr(format!("{} prepare meta: failed: {:?}", file_id, err))
                })?;
                let bytes_cloned = bytes.clone();
                let save_res = runtime.spawn_blocking(move || {
                    Self::save_table_meta(file_id, &local_path, &bytes_cloned)
                });
                if let Err(err) = save_res.await.unwrap() {
                    debug_assert!(false, "{} save table meta failed: {:?}", file_id, err);
                    warn!("{} prepare meta: write failed", file_id; "err" => ?err);
                }
                ia_mgr.access_table_meta(file_id);
                bytes
            }
            Err(err) => {
                return Err(Error::IaMgr(format!(
                    "{} prepare meta: read failed: {:?}",
                    file_id, err
                )));
            }
        };
        Ok(table_meta_data)
    }

    fn save_table_meta(file_id: u64, local_path: &Path, data: &[u8]) -> Result<()> {
        lazy_static::lazy_static! {
            static ref TMP_ID: AtomicU64 = AtomicU64::new(0);
        }
        let tmp_path = local_path.with_extension(format!(
            "{}.{}.tmp",
            TABLE_META_LOCAL_FILE_SUFFIX,
            TMP_ID.fetch_add(1, Relaxed)
        ));
        let mut f = fs::File::create(&tmp_path).table_ctx(file_id, "create_tmp")?;
        f.write_all(data).table_ctx(file_id, "write_tmp")?;
        f.sync_data().table_ctx(file_id, "sync_tmp")?;
        fs::rename(&tmp_path, local_path).table_ctx(file_id, "rename")?;
        Ok(())
    }

    pub(crate) fn table_meta_file(&self) -> &Arc<dyn File> {
        &self.table_meta_file
    }

    /// Mmaps part of a remote file.
    ///
    /// WARNING: The returned byte slice does not have any alignment guarantees
    /// because we will actually read by segments. However If your requested
    /// offset is properly aligned within a segment, then the returned slice
    /// will be properly aligned.
    ///
    /// WARNING: As a IaFile, the returned bytes may either be in-memory or
    /// on-disk. The returned bytes is always accessible. However it is not
    /// wise to keep the returned bytes for a long time, because data may
    /// be moved from memory into disk. If you keep the returned bytes,
    /// then the memory is always pinned in memory, which may cause
    /// OOM. Also, the data may be evicted on disk, but the disk space
    /// will not be released while bytes are still kept.
    /// For this reason, a `IaMmapSource` is additionally returned, so that
    /// you can check whether the returned bytes shall be dropped to free
    /// the underlying resource.
    pub async fn mmap_range(&self, offset: u64, length: usize) -> Result<(Bytes, IaMmapSource)> {
        let ident = self.align_to_segment(offset, offset + length as u64)?;
        let segment_handle = self.mgr.get_segment_handle(ident, self.ftype, None).await?;

        // The file here may be in-memory or on-disk. It will be never remote.
        let segment_file = segment_handle.into_inner();
        let mmap = segment_file.mmap2()?;

        // This is unexpected, possibly caused by wrong segment offsets.
        if offset < ident.start_off || offset + length as u64 > ident.end_off {
            return Err(Error::IaMgr(format!(
                "{} mmap segment: out of range: offset {}, length {}, ident {}",
                self.id, offset, length, ident
            )));
        }

        let offset_in_segment = (offset - ident.start_off) as usize;
        let sliced = mmap.slice(offset_in_segment..(offset_in_segment + length));
        let source = IaMmapSource {
            ident,
            mgr: self.mgr.clone(),
            is_source_in_memory: segment_file.path().is_none(),
        };
        Ok((sliced, source))
    }
}

#[async_trait]
impl File for IaFile {
    fn id(&self) -> u64 {
        self.id
    }

    fn size(&self) -> u64 {
        self.size
    }

    fn is_sync(&self) -> bool {
        false
    }

    fn read(&self, off: u64, length: usize) -> Result<Bytes> {
        let (_guard, timeout): (Arc<()>, _) = self.mgr.acquire_sync_read().expect("acquire");
        let this = self.clone();
        let res = tokio::task::block_in_place(move || {
            let task = async move {
                let res = tokio::time::timeout(timeout, this.read_async(off, length)).await;
                drop(_guard);
                res
            };
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.block_on(task)
            } else {
                self.mgr.runtime_handle().block_on(task)
            }
        })
        .expect("timeout");

        ENGINE_IA_SYNC_READ_COUNTER.inc();
        let bt = backtrace::Backtrace::new();
        warn!("sync read IA file"; "id" => self.id(), "off" => off, "len" => length, "bt" => ?bt);
        res
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        let data = self.read(offset, buf.len())?;
        buf.copy_from_slice(&data);
        Ok(())
    }

    fn read_table_meta(&self, off: u64, length: usize) -> Result<Bytes> {
        if off < self.table_meta_off || off + length as u64 > self.size {
            error!("{} read table meta: out of range", self.id;
                "off" => off, "length" => length, "file" => ?self);
            return Err(Error::IaMgr(format!(
                "{} read table meta: out of range",
                self.id
            )));
        }
        self.table_meta_file.read(off - self.table_meta_off, length)
    }

    async fn read_async(&self, off: u64, length: usize) -> Result<Bytes> {
        if off >= self.table_meta_off {
            return self.read_table_meta(off, length);
        }

        let mut buf = BytesMut::new();
        buf.resize(length, 0);
        let read_at = ReadAt::new(buf.as_mut(), off, false);
        self.read_range(read_at).await?;
        Ok(buf.freeze())
    }

    async fn read_at_async(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        let buf_len = buf.len();
        if offset >= self.table_meta_off {
            buf.copy_from_slice(&self.read_table_meta(offset, buf_len)?);
            return Ok(());
        }

        let read_at = ReadAt::new(buf, offset, false);
        self.read_range(read_at).await
    }

    fn mmap(&self) -> Result<MmapData> {
        unimplemented!()
    }

    fn mmap2(&self) -> Result<Bytes> {
        unimplemented!()
    }

    async fn mmap_async(&self) -> Result<MmapData> {
        let file_size = self.size();
        if file_size == 0 {
            return Err(Error::Other("file size is 0".to_string()));
        }

        assert!(self.table_meta_off > 0);
        let ident = FileSegmentIdent {
            file_id: self.id(),
            start_off: 0,
            end_off: self.table_meta_off,
        };

        let segment_handle = self.mgr.get_segment_handle(ident, self.ftype, None).await?;

        let file = segment_handle.into_inner();
        Ok(file.mmap()?.to_aligned())
    }

    async fn mmap_range(&self, offset: u64, length: usize) -> Result<(Bytes, FileMmapGuard)> {
        let (data, guard) = IaFile::mmap_range(self, offset, length).await?;
        Ok((data, FileMmapGuard::Ia(guard)))
    }

    fn get_remote_segments(
        &self,
        ranges: &[(u64 /* start_off */, u64 /* end_off */)],
    ) -> Result<(Vec<FileSegmentIdent>, usize /* total_segments */)> {
        if ranges.is_empty() {
            return Ok((vec![], 0));
        }
        let mut total_segments = 0;
        let mut segments = vec![];
        let mut ident_set = HashSet::new();
        for &(start_off, end_off) in ranges {
            let mut off = start_off;
            while off < end_off {
                let ident = self.align_to_segment(off, off + 1)?;
                off = ident.end_off;
                if ident_set.contains(&ident.start_off) {
                    continue;
                }
                ident_set.insert(ident.start_off);
                if !self.mgr.is_segment_cached(ident) {
                    segments.push(ident);
                }
                total_segments += 1;
            }
        }
        Ok((segments, total_segments))
    }

    fn get_segment_ident(&self, offset: u64) -> Result<FileSegmentIdent> {
        align_to_segment(self.id, &self.segment_offsets, offset, offset + 1)
    }

    fn storage_class(&self) -> StorageClass {
        StorageClass::Ia
    }

    fn as_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn mem_size(&self) -> u64 {
        self.table_meta_file.size() + self.segment_offsets.len() as u64 * 8 + 8 * 3 + 1
    }
}

/// Used to indicate whether the underlying entity of mmapped bytes from IaFile
/// are not valid any more. When the underlying entity is not valid, the mmapped
/// bytes is still accessible, but is encouraged to be dropped in order to free
/// memory or free file handle.
#[derive(Clone)]
pub struct IaMmapSource {
    ident: FileSegmentIdent,
    mgr: IaManager,
    is_source_in_memory: bool,
}

impl IaMmapSource {
    pub fn is_valid(&self) -> bool {
        match self.mgr.segment_cached_position(self.ident) {
            FileSegmentPosition::InMem => self.is_source_in_memory,
            FileSegmentPosition::InStore => !self.is_source_in_memory,
            FileSegmentPosition::NotExist => false,
        }
    }
}

struct SegmentOffsetsBuilder {
    segment_size: u64,
    offsets: Vec<u64>,
    last_off: u64,
}

impl SegmentOffsetsBuilder {
    fn new(segment_size: u64) -> Self {
        debug_assert!(segment_size > 0);
        Self {
            segment_size,
            offsets: vec![0],
            last_off: 0,
        }
    }

    fn push_block_off(&mut self, off: u64) {
        if off >= self.last_off + self.segment_size {
            self.push_boundary(off);
        }
    }

    fn push_boundary(&mut self, off: u64) {
        debug_assert!(off > self.last_off);
        self.offsets.push(off);
        self.last_off = off;
    }

    fn push_from_sstable_idx(&mut self, idx: &sstable::Index) {
        for pos in 0..idx.num_blocks() {
            let addr = idx.get_block_addr(pos);
            self.push_block_off(addr.curr_off as u64);
        }
    }

    fn push_from_columnar_table_meta(&mut self, meta: &TableMeta) {
        // Since the workload likely to read the whole column, we split the segment by
        // the column boundary.
        // handle column
        let (pack_start, _) = meta.handle_column.pack_offsets.get(0);
        self.push_block_off(pack_start as u64);
        // version column
        let (pack_start, _) = meta.version_column.pack_offsets.get(0);
        self.push_block_off(pack_start as u64);
        // columns
        let mut unordered_offsets = vec![];
        for col in meta.columns.values() {
            let (pack_start, _) = col.pack_offsets.get(0);
            unordered_offsets.push(pack_start as u64);
        }
        unordered_offsets.sort();
        for off in unordered_offsets {
            self.push_block_off(off);
        }
    }

    fn finish(self) -> Vec<u64> {
        self.offsets
    }
}

fn align_to_segment(
    file_id: u64,
    segment_offsets: &[u64],
    start_off: u64,
    end_off: u64,
) -> Result<FileSegmentIdent> {
    debug_assert!(!segment_offsets.is_empty());

    let segment_len = segment_offsets.len();
    if end_off <= start_off
        || start_off < segment_offsets[0]
        || segment_offsets[segment_len - 1] < end_off
    {
        return Err(Error::IaMgr(format!(
            "{} read out of range: start {}, end {}",
            file_id, start_off, end_off
        )));
    }

    let pos = search(segment_len, |i| start_off < segment_offsets[i]) - 1;
    if segment_offsets[pos + 1] < end_off {
        return Err(Error::IaMgr(format!(
            "{} read more than one segment: start {}, end {}",
            file_id, start_off, end_off
        )));
    }

    Ok(FileSegmentIdent {
        file_id,
        start_off: segment_offsets[pos],
        end_off: segment_offsets[pos + 1],
    })
}

pub fn table_meta_file_local_path(file_id: u64, file_type: FileType, data_dir: &Path) -> PathBuf {
    match file_type {
        FileType::Sst => data_dir.join(format!(
            "{}.{}",
            new_sst_filename(file_id).display(),
            TABLE_META_LOCAL_FILE_SUFFIX
        )),
        FileType::Columnar => data_dir.join(format!(
            "{}.{}",
            new_columnar_filename(file_id).display(),
            TABLE_META_LOCAL_FILE_SUFFIX
        )),
        FileType::VectorIndex => data_dir.join(format!(
            "{}.{}",
            new_vector_index_filename(file_id).display(),
            TABLE_META_LOCAL_FILE_SUFFIX
        )),
        FileType::FtsPackedFile => data_dir.join(format!(
            "{}.{}",
            new_fts_packed_filename(file_id).display(),
            TABLE_META_LOCAL_FILE_SUFFIX
        )),
        FileType::FtsDedicatedFile => data_dir.join(format!(
            "{}.{}",
            new_fts_dedicated_filename(file_id).display(),
            TABLE_META_LOCAL_FILE_SUFFIX
        )),
        FileType::Blob => data_dir.join(format!(
            "{}.{}",
            new_blob_filename(file_id).display(),
            TABLE_META_LOCAL_FILE_SUFFIX
        )),
        _ => unimplemented!("file type not supported: {:?}", file_type),
    }
}

// Format: {:016x}.{:file_type}.meta
pub fn parse_table_meta_filename(filename: &str) -> Option<(u64 /* file_id */, FileType)> {
    let parts: Vec<&str> = filename.split('.').collect();
    if parts.len() != 3 || parts[2] != TABLE_META_LOCAL_FILE_SUFFIX {
        return None;
    }
    let file_id = u64::from_str_radix(parts[0], 16).ok()?;
    let file_type = FileType::try_from(parts[1]).ok()?;
    Some((file_id, file_type))
}

#[cfg(any(test, feature = "testexport"))]
impl IaFile {
    pub fn open_in_path(id: u64, fm: &FileMeta, data_dir: &Path, mgr: IaManager) -> Result<Self> {
        use crate::table::file::LocalFile;

        // mtime is set during prepare.
        let table_meta_file =
            LocalFile::open(id, table_meta_file_local_path(id, fm.file_type, data_dir)).map_err(
                |err| Error::IaMgr(format!("{} open: open meta file failed: {:?}", id, err)),
            )?;
        Self::open(id, fm, Arc::new(table_meta_file), mgr)
    }

    pub async fn multi_read_async(&self, off: u64, length: usize) -> Result<Bytes> {
        let mut segments = vec![];
        let end_off = off + length as u64;
        let mut seg_off = off;
        while seg_off < end_off {
            let ident = self.align_to_segment(seg_off, seg_off + 1)?;
            seg_off = ident.end_off;
            segments.push(ident);
        }

        Ok(if segments.len() == 1 {
            self.read_async(off, length).await?
        } else {
            let mut bytes = BytesMut::with_capacity(length);
            let segments_len = segments.len();
            let mut handles = Vec::with_capacity(segments_len);
            for (idx, ident) in segments.into_iter().enumerate() {
                let seg_start_off = if idx == 0 { off } else { ident.start_off };
                let seg_end_off = if idx == segments_len - 1 {
                    end_off
                } else {
                    ident.end_off
                };
                let seg_len = seg_end_off - seg_start_off;
                let ia_file = self.clone();
                let task = async move { ia_file.read_async(seg_start_off, seg_len as usize).await };
                handles.push(tokio::spawn(task));
            }

            let res = futures::future::try_join_all(handles).await.unwrap();
            for r in res {
                let r = r?;
                bytes.extend_from_slice(&r);
            }
            bytes.freeze()
        })
    }

    pub async fn multi_read_at_async(&self, mut buf: &mut [u8], off: u64) -> Result<()> {
        use bytes::BufMut as _;

        let mut segments = vec![];
        let end_off = off + buf.len() as u64;
        let mut seg_off = off;
        while seg_off < end_off {
            let ident = self.align_to_segment(seg_off, seg_off + 1)?;
            seg_off = ident.end_off;
            segments.push(ident);
        }

        if segments.len() == 1 {
            self.read_at_async(buf, off).await?;
        } else {
            let segments_len = segments.len();
            let mut handles = Vec::with_capacity(segments_len);
            for (idx, ident) in segments.into_iter().enumerate() {
                let seg_start_off = if idx == 0 { off } else { ident.start_off };
                let seg_end_off = if idx == segments_len - 1 {
                    end_off
                } else {
                    ident.end_off
                };
                let seg_len = seg_end_off - seg_start_off;
                let mut seg_buf = vec![0; seg_len as usize];
                let ia_file = self.clone();
                let task = async move {
                    ia_file
                        .read_at_async(seg_buf.as_mut_slice(), seg_start_off)
                        .await
                        .map(|()| seg_buf)
                };
                handles.push(tokio::spawn(task));
            }

            let res = futures::future::try_join_all(handles).await.unwrap();
            for r in res {
                let r = r?;
                buf.put_slice(&r);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_align_to_segment() {
        let segment_offsets = vec![0, 100, 200, 300];
        let cases = [
            // start_off, end_off, expected
            (0, 1, Some((0, 100))),
            (0, 100, Some((0, 100))),
            (0, 101, None),
            (99, 100, Some((0, 100))),
            (100, 101, Some((100, 200))),
            (101, 102, Some((100, 200))),
            (200, 300, Some((200, 300))),
            (299, 300, Some((200, 300))),
            (0, 300, None),
            (0, 1000, None),
            (1, 0, None),
        ];
        for (start, end, expected) in cases {
            let expected = expected.map(|(start_off, end_off)| FileSegmentIdent {
                file_id: 1,
                start_off,
                end_off,
            });
            assert_eq!(
                align_to_segment(1, &segment_offsets, start, end).ok(),
                expected,
            );
        }
    }

    #[test]
    fn test_table_meta_filename() {
        let data_dir = PathBuf::from("/data");
        let cases = vec![
            (42, FileType::Sst, "/data/000000000000002a.sst.meta"),
            (43, FileType::Columnar, "/data/000000000000002b.col.meta"),
        ];
        for (file_id, file_type, expected) in cases {
            let path = table_meta_file_local_path(file_id, file_type, &data_dir);
            assert_eq!(path.display().to_string(), expected);

            let filename = path.as_path().file_name().unwrap().to_str().unwrap();
            assert_eq!(
                parse_table_meta_filename(filename).unwrap(),
                (file_id, file_type)
            );
        }

        let invalid_cases = vec![
            "000000000000002a.sst",
            "000000000000002a.sst.meta1",
            "000000000000002a.s.meta",
            "x00000000000002a.sst.meta",
        ];
        for filename in invalid_cases {
            assert!(parse_table_meta_filename(filename).is_none());
        }
    }
}

// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashSet, ops::Deref, sync::Arc};

use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes};
use cloud_encryption::EncryptionKey;
use itertools::Itertools;
use tidb_query_datatype::codec::{
    mysql::{VectorFloat32Encoder, VectorFloat32Ref},
    table::{encode_common_handle_row_key, encode_row_key},
};
use tipb::VectorDistanceMetric;
use usearch::IndexOptions;

use crate::{
    ia::types::FileSegmentIdent,
    table::{
        self,
        columnar::{
            get_fixed_size, Block, ColumnarConcatReader, ColumnarFile, ColumnarLevels,
            ColumnarMergeReader, ColumnarReader, ColumnarTableReader,
        },
        file::{File, MmapData},
        schema_file::Schema,
        search,
        vector_index::{
            vector_distance_projector::{VectorDistanceProjector, VIRTUAL_DISTANCE_COLUMN_ID},
            VectorIndexCache,
        },
        BoundedDataSet, DataBound, Error,
        Error::Other,
        InnerKey, Result, SnapVersion,
    },
};

const MAGIC_NUMBER: u32 = 0x19504cf0;
const FORMAT_VERSION: u32 = 1;
const FORMAT_VERSION_V2: u32 = 2; // Add mvcc delete info.
const CONNECTIVITY: usize = 16;
const EXPANSION_ADD: usize = 128;
const EXPANSION_SEARCH: usize = 64;

const QUANTIZATION: usearch::ScalarKind = usearch::ScalarKind::F32;

#[derive(Default, Clone)]
pub struct VectorIndexes {
    indexes: Vec<VectorIndex>,
}

impl VectorIndexes {
    pub(crate) fn is_empty(&self) -> bool {
        self.indexes.is_empty()
    }

    pub(crate) fn get_all(&self) -> &[VectorIndex] {
        &self.indexes
    }

    pub(crate) fn get_mut_all(&mut self) -> &mut [VectorIndex] {
        &mut self.indexes
    }

    pub fn add_index_file(&mut self, vec_idx_file: VectorIndexFile) {
        for index in &mut self.indexes {
            if index.table_id == vec_idx_file.table_id()
                && index.index_id == vec_idx_file.index_id()
                && index.col_id == vec_idx_file.column_id()
            {
                if !index
                    .files
                    .iter()
                    .any(|f| f.file_id() == vec_idx_file.file_id())
                {
                    index.files.push(vec_idx_file);
                    index.sort();
                }
                return;
            }
        }
        let mut index = VectorIndex::new(
            vec_idx_file.table_id(),
            vec_idx_file.index_id(),
            vec_idx_file.column_id(),
        );
        index.files.push(vec_idx_file);
        self.indexes.push(index);
    }

    pub fn remove_index_file(
        &mut self,
        table_id: i64,
        index_id: i64,
        col_id: i64,
        remove_file_ids: &[u64],
    ) {
        for index in &mut self.indexes {
            if index.table_id == table_id && index.index_id == index_id && index.col_id == col_id {
                index
                    .files
                    .retain(|f| !remove_file_ids.contains(&f.file_id()));
                break;
            }
        }
        self.indexes.retain(|index| !index.files.is_empty());
    }

    pub fn iter(&self) -> impl Iterator<Item = &VectorIndex> {
        self.indexes.iter()
    }

    pub fn get(&self, table_id: i64, index_id: i64, col_id: i64) -> Option<&VectorIndex> {
        self.indexes.iter().find(|vec_idx| {
            vec_idx.table_id == table_id && vec_idx.index_id == index_id && vec_idx.col_id == col_id
        })
    }

    pub fn get_mut(
        &mut self,
        table_id: i64,
        index_id: i64,
        col_id: i64,
    ) -> Option<&mut VectorIndex> {
        self.indexes
            .iter_mut()
            .find(|i| i.table_id == table_id && i.index_id == index_id && i.col_id == col_id)
    }

    pub fn update_snap_version(
        &mut self,
        table_id: i64,
        index_id: i64,
        col_id: i64,
        snap_version: SnapVersion,
    ) {
        if let Some(index) = self.get_mut(table_id, index_id, col_id) {
            debug_assert!(
                index.snap_version <= snap_version,
                "old snap_version: {}, new snap_version: {}",
                index.snap_version,
                snap_version
            );
            index.set_snap_version(snap_version);
        }
    }

    pub fn sort(&mut self) {
        for index in &mut self.indexes {
            index.sort();
        }
    }

    pub fn retain(&mut self, f: impl Fn(&VectorIndex) -> bool) {
        self.indexes.retain(f);
    }
}

#[derive(Clone)]
pub struct VectorIndex {
    pub table_id: i64,
    pub index_id: i64,
    pub col_id: i64,
    pub snap_version: SnapVersion,
    pub(crate) files: Vec<VectorIndexFile>,
    pub(crate) extra_columnar_files: Vec<u64>,
}

impl VectorIndex {
    pub(crate) fn new(table_id: i64, index_id: i64, col_id: i64) -> VectorIndex {
        VectorIndex {
            table_id,
            index_id,
            col_id,
            snap_version: SnapVersion::zero(),
            files: vec![],
            extra_columnar_files: vec![],
        }
    }

    // Sort the vector index files stable. If the snap_version is the same,
    // we sort the files by file_id to make the order stable.
    pub(crate) fn sort(&mut self) {
        self.files.sort_by(|a, b| {
            b.snap_version()
                .cmp(&a.snap_version())
                .then_with(|| b.file_id().cmp(&a.file_id()))
        });
    }

    pub fn set_snap_version(&mut self, snap_version: SnapVersion) {
        self.snap_version = snap_version;
    }

    pub fn snap_version(&self) -> SnapVersion {
        // For backward compatibility, if the snap version is 0, we use the snap version
        // of the first file.
        if self.snap_version.is_zero() && !self.files.is_empty() {
            return self.files[0].snap_version();
        }
        self.snap_version
    }

    /// The number of vector index files with different snap version.
    /// The vector index file maybe split into multiple files if the size exceed
    /// the max file size. We count the number of files with different snap
    /// version.
    pub fn files_count(&self) -> usize {
        self.files.iter().map(|f| f.snap_version()).unique().count()
    }

    pub(crate) fn update_extra_columnar_files(&mut self, col_levels: &ColumnarLevels) {
        self.extra_columnar_files.clear();
        for col_level in &col_levels.levels {
            if col_level.level == 2 {
                continue;
            }
            for file in &col_level.files {
                if !file.has_table(self.table_id) {
                    continue;
                }
                if self.files.iter().any(|vec_idx_file| {
                    vec_idx_file.snap_version() > file.get_snap_version().unwrap_or_default()
                        && !vec_idx_file.overlap_bound(file.data_bound())
                }) {
                    self.extra_columnar_files.push(file.id());
                }
            }
        }
    }

    pub(crate) fn contains_columnar_file(&self, col_file: &ColumnarFile) -> bool {
        if self.extra_columnar_files.contains(&col_file.id()) {
            return false;
        }
        if col_file.get_snap_version().unwrap_or_default() <= self.snap_version() {
            return true;
        }
        false
    }

    pub(crate) fn need_rebuild(&self) -> bool {
        !self.extra_columnar_files.is_empty()
    }

    pub async fn search(
        &self,
        target: &[f32],
        count: usize,
        start_ts: u64,
        start_handle: Option<&[u8]>,
        end_handle: Option<&[u8]>,
        is_common_handle: bool,
        enable_distance_proj: bool,
    ) -> Result<Vec<VectorItem>> {
        let mut results = vec![];
        let mut handles_dedup = HashSet::new();
        let mut newer_index_files = vec![];
        for file in &self.files {
            let (items, deleted_handles) = file
                .search(
                    target,
                    count,
                    start_ts,
                    enable_distance_proj,
                    &newer_index_files,
                )
                .await?;
            // Add the deleted handles to the dedup set to avoid read the same handle in
            // next VectorIndexFile.
            handles_dedup.extend(deleted_handles);

            for item in items {
                if handles_dedup.contains(&item.handle) {
                    continue;
                }
                handles_dedup.insert(item.handle.clone());
                results.push(item);
            }
            newer_index_files.push(file.clone());
        }

        // Filter the results by the start_handle and end_handle.
        if let Some(start_handle) = start_handle {
            results.retain(|item| {
                let mut handle = item.handle.as_slice();
                if is_common_handle {
                    handle >= start_handle && end_handle.map_or(true, |e| handle < e)
                } else {
                    let mut start_handle = start_handle;
                    let int_handle = handle.get_i64_le();
                    let start_int_handle = start_handle.get_i64_le();
                    let end_int_handle = end_handle.map(|mut h| h.get_i64_le());
                    int_handle >= start_int_handle
                        && end_int_handle.map_or(true, |e| int_handle < e)
                }
            });
        }

        // Select n_th by distance.
        if count < results.len() {
            results
                .select_nth_unstable_by(count, |a, b| a.distance.partial_cmp(&b.distance).unwrap());
            results.truncate(count);
        }

        // Sort by handle.
        if is_common_handle {
            results.sort_by(|a, b| a.handle.cmp(&b.handle));
        } else {
            results.sort_by(|a, b| {
                a.handle
                    .as_slice()
                    .get_i64_le()
                    .cmp(&b.handle.as_slice().get_i64_le())
            })
        }
        Ok(results)
    }

    pub fn to_vector_index_pb(&self) -> kvenginepb::VectorIndex {
        let mut vec_idx_pb = kvenginepb::VectorIndex::new();
        vec_idx_pb.set_table_id(self.table_id);
        vec_idx_pb.set_index_id(self.index_id);
        vec_idx_pb.set_col_id(self.col_id);
        vec_idx_pb.set_snap_version(self.snap_version.into_inner());
        for vec_idx_file in &self.files {
            vec_idx_pb
                .mut_files()
                .push(vec_idx_file.to_vector_index_file_pb());
        }
        vec_idx_pb
    }
}

// index file format:
// V1(DEPRECATED):
//   index | versions | handles | props | footer
// V2:
//   index | versions | handles | nulls | props | footer
#[derive(Debug, Clone)]
#[repr(C)]
pub struct VectorIndexFileFooter {
    index_size: u32,
    props_size: u32,
    format_ver: u32,
    magic_number: u32,
}

impl Default for VectorIndexFileFooter {
    fn default() -> Self {
        VectorIndexFileFooter {
            index_size: 0,
            props_size: 0,
            format_ver: FORMAT_VERSION_V2,
            magic_number: MAGIC_NUMBER,
        }
    }
}

impl VectorIndexFileFooter {
    pub fn unmarshal(&mut self, mut data: &[u8]) {
        self.index_size = data.get_u32_le();
        self.props_size = data.get_u32_le();
        self.format_ver = data.get_u32_le();
        self.magic_number = data.get_u32_le();
    }

    fn marshal(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::size());
        buf.put_u32_le(self.index_size);
        buf.put_u32_le(self.props_size);
        buf.put_u32_le(self.format_ver);
        buf.put_u32_le(MAGIC_NUMBER);
        buf
    }

    pub fn size() -> usize {
        std::mem::size_of::<VectorIndexFileFooter>()
    }

    pub fn props_size(&self) -> usize {
        self.props_size as usize
    }

    fn valid_version(&self) -> bool {
        self.format_ver == FORMAT_VERSION || self.format_ver == FORMAT_VERSION_V2
    }

    fn format_version(&self) -> u32 {
        self.format_ver
    }
}

#[derive(Clone)]
pub struct VectorIndexFile {
    core: Arc<VectorIndexFileCore>,
}

impl VectorIndexFile {
    fn is_loaded(&self) -> bool {
        self.core.index_data.get().is_some()
    }

    fn index_size(&self) -> u32 {
        self.footer.index_size
    }

    fn index(&self) -> &usearch::Index {
        &self.core.index_data.get().unwrap().index
    }

    fn index_data(&self) -> &IndexData {
        self.core.index_data.get().unwrap()
    }

    pub fn file_id(&self) -> u64 {
        self.core.file.id()
    }

    pub fn file_size(&self) -> u64 {
        self.core.file.size()
    }

    pub fn snap_version(&self) -> SnapVersion {
        self.core.snap_version
    }

    pub fn table_id(&self) -> i64 {
        self.core.table_id
    }

    pub fn index_id(&self) -> i64 {
        self.core.index_id as i64
    }

    pub fn column_id(&self) -> i64 {
        self.core.column_id as i64
    }

    pub fn smallest(&self) -> InnerKey<'_> {
        InnerKey::from_inner_buf(&self.core.smallest)
    }

    pub fn biggest(&self) -> InnerKey<'_> {
        InnerKey::from_inner_buf(&self.core.biggest)
    }

    pub fn meta_offset(&self) -> u32 {
        self.core.meta_offset
    }

    pub fn is_legacy_format(&self) -> bool {
        self.footer.format_version() == FORMAT_VERSION || self.meta_offset() == 0
    }

    pub fn contains_handle(&self, handle: &[u8]) -> bool {
        let len = self.get_versions().len();
        let idx = if self.is_common_handle {
            search(len, |i| self.get_handle(i as u64) >= handle)
        } else {
            let mut handle = handle;
            let int_handle = handle.get_i64_le();
            search(len, |i| {
                self.get_handle(i as u64).get_i64_le() >= int_handle
            })
        };
        idx < len && self.get_handle(idx as u64) == handle
    }
}

impl Deref for VectorIndexFile {
    type Target = VectorIndexFileCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl BoundedDataSet for VectorIndexFile {
    fn data_bound(&self) -> DataBound<'_> {
        DataBound::new(self.smallest(), self.biggest(), true)
    }
}

pub struct VectorIndexFileCore {
    file: Arc<dyn File>,
    footer: VectorIndexFileFooter,
    snap_version: SnapVersion,
    table_id: i64,
    index_id: i32,
    column_id: i32,
    is_common_handle: bool,
    meta_offset: u32,
    smallest: Vec<u8>,
    biggest: Vec<u8>,
    metric_repr: i32,
    index_data: tokio::sync::OnceCell<Arc<IndexData>>,
    vector_index_cache: Option<VectorIndexCache>,
}

pub struct IndexData {
    file_data: MmapData,
    index: usearch::Index,
    versions_start: u32,
    versions_end: u32,
    handles_start: u32,
    handles_end: u32,
    nulls_start: u32,
    nulls_end: u32,
}

impl IndexData {
    /// Returns the size of the index data resident in memory.
    pub fn index_size(&self) -> usize {
        match &self.file_data {
            // Memory-mapped files don't consume heap memory equal to file size.
            // The OS pages data in/out on demand. We only count the IndexData struct overhead.
            MmapData::Local(_) => std::mem::size_of::<IndexData>(),
            MmapData::InMem(data) => data.len(),
            MmapData::AlignedInMem(data) => data.len(),
        }
    }

    pub fn is_in_mem(&self) -> bool {
        match &self.file_data {
            MmapData::Local(_) => false,
            MmapData::InMem(_) => true,
            MmapData::AlignedInMem(_) => true,
        }
    }
}

impl VectorIndexFile {
    pub fn new(
        file: Arc<dyn File>,
        meta_offset: u32,
        vector_index_cache: Option<VectorIndexCache>,
    ) -> Result<Self> {
        let file_len = file.size();
        let mut footer = VectorIndexFileFooter::default();
        let footer_len = VectorIndexFileFooter::size();
        let footer_offset = file_len - footer_len as u64;
        let footer_data = file.read_footer(footer_len)?;
        footer.unmarshal(footer_data.chunk());
        if footer.magic_number != MAGIC_NUMBER {
            return Err(Error::InvalidMagicNumber);
        }
        if !footer.valid_version() {
            return Err(Other(format!(
                "unsupported format version: {}",
                footer.format_ver
            )));
        }
        let prop_size = footer.props_size as usize;
        let prop_offset = footer_offset - prop_size as u64;
        let prop_data = file.read_table_meta(prop_offset, prop_size)?;
        let mut prop_data_buf = prop_data.chunk();
        let mut snap_version = SnapVersion::zero();
        let mut table_id = 0;
        let mut index_id = 0;
        let mut column_id = 0;
        let mut metric_repr = 0;
        let mut is_common_handle = false;
        let mut smallest = vec![];
        let mut biggest = vec![];
        while prop_data_buf.len() > 2 {
            let prop_key_len = prop_data_buf.get_u16_le();
            if prop_key_len == 0 {
                break;
            }
            let prop_key = &prop_data_buf[..prop_key_len as usize];
            prop_data_buf.advance(prop_key_len as usize);
            let prop_val_len = prop_data_buf.get_u32_le();
            let mut prop_value = &prop_data_buf[..prop_val_len as usize];
            prop_data_buf.advance(prop_val_len as usize);
            if prop_key == PROP_SMALLEST.as_bytes() {
                smallest = prop_value.to_vec();
            } else if prop_key == PROP_BIGGEST.as_bytes() {
                biggest = prop_value.to_vec();
            } else if prop_key == PROP_TABLE_ID.as_bytes() {
                table_id = prop_value.get_i64_le();
            } else if prop_key == PROP_INDEX_ID.as_bytes() {
                index_id = prop_value.get_i32_le();
            } else if prop_key == PROP_COLUMN_ID.as_bytes() {
                column_id = prop_value.get_i32_le();
            } else if prop_key == PROP_IS_COMMON_HANDLE.as_bytes() {
                is_common_handle = prop_value.get_u8() == 1;
            } else if prop_key == PROP_METRIC_REPR.as_bytes() {
                metric_repr = prop_value.get_i32_le();
            } else if prop_key == PROP_SNAP_VERSION.as_bytes() {
                snap_version = prop_value.get_u64_le().into();
            }
        }
        Ok(VectorIndexFile {
            core: Arc::new(VectorIndexFileCore {
                file,
                footer,
                smallest,
                biggest,
                snap_version,
                table_id,
                index_id,
                column_id,
                is_common_handle,
                meta_offset,
                metric_repr,
                index_data: tokio::sync::OnceCell::new(),
                vector_index_cache,
            }),
        })
    }

    pub async fn load_data(&self) -> Result<()> {
        if self.is_loaded() {
            return Ok(());
        }
        self.load_data_inner().await?;
        Ok(())
    }

    async fn load_data_inner(&self) -> Result<()> {
        if let Some(vector_index_cache) = &self.core.vector_index_cache {
            if let Some(index_data) = vector_index_cache.get(self.file_id()) {
                self.core.index_data.set(index_data.clone()).map_err(|_| {
                    Error::Other("Failed to set vector index loaded data to cache".to_string())
                })?;
                return Ok(());
            }
        }
        let file_data = if self.file.is_sync() {
            self.file.mmap()?
        } else {
            self.file.mmap_async().await?
        };

        let index_data = &file_data[..self.index_size() as usize];
        let mut opts = new_index_opts();
        opts.metric.repr = self.metric_repr;
        let index = usearch::Index::new(&opts).map_err(|e| Other(e.to_string()))?;
        unsafe {
            index
                .view_from_buffer(index_data)
                .map_err(|e| Other(e.to_string()))?;
        }
        let versions_start = (index_data.len() + 7) & !7; // aligned to 8 bytes
        let versions_end = versions_start + index.size() * 8;
        let mut handles_start = versions_end;
        let handles_end;
        if self.is_common_handle {
            handles_start = versions_end + (index.size() + 1) * 4;
            let handle_offsets = &file_data[versions_end..handles_start];
            let handle_data_length =
                (&handle_offsets[handle_offsets.len() - 4..]).get_u32_le() as usize;
            handles_end = handles_start + handle_data_length;
        } else {
            handles_end = handles_start + index.size() * 8;
        };
        let mut nulls_start = 0;
        let mut nulls_end = 0;
        if self.footer.format_version() == FORMAT_VERSION_V2 {
            nulls_start = handles_end;
            nulls_end = nulls_start + index.size();
        }

        let index_data = Arc::new(IndexData {
            index,
            file_data,
            versions_start: versions_start as u32,
            versions_end: versions_end as u32,
            handles_start: handles_start as u32,
            handles_end: handles_end as u32,
            nulls_start: nulls_start as u32,
            nulls_end: nulls_end as u32,
        });

        self.core
            .index_data
            .set(index_data.clone())
            .map_err(|_| Error::Other("Failed to set loaded data".to_string()))?;
        if let Some(vector_index_cache) = &self.core.vector_index_cache {
            let ident = FileSegmentIdent {
                file_id: self.file_id(),
                start_off: 0,
                end_off: self.meta_offset() as u64,
            };
            vector_index_cache.insert(self.file_id(), index_data, ident);
        }

        Ok(())
    }

    fn get_versions(&self) -> &[u64] {
        let index_data = self.index_data();
        bytemuck::cast_slice(
            &index_data.file_data
                [index_data.versions_start as usize..index_data.versions_end as usize],
        )
    }

    fn get_handles_offsets(&self) -> &[u32] {
        let index_data = self.index_data();
        bytemuck::cast_slice(
            &index_data.file_data
                [index_data.versions_end as usize..index_data.handles_start as usize],
        )
    }

    fn get_handles_data(&self) -> &[u8] {
        let index_data = self.index_data();
        bytemuck::cast_slice(
            &index_data.file_data
                [index_data.handles_start as usize..index_data.handles_end as usize],
        )
    }

    pub(crate) fn has_nulls(&self) -> bool {
        let index_data = self.index_data();
        index_data.nulls_start != 0 && index_data.nulls_end != 0
    }

    fn is_null(&self, key: usize) -> bool {
        // For index file v1 format, there is no nulls, ignore this check.
        // TODO: Remove this check after all the index files are migrated to v2 format.
        if !self.has_nulls() {
            return false;
        }
        let index_data = self.index_data();
        index_data.file_data[index_data.nulls_start as usize + key] == 1
    }

    fn get_handle(&self, key: u64) -> &[u8] {
        let handles = self.get_handles_data();
        if self.is_common_handle {
            let handle_offsets = self.get_handles_offsets();
            let start = handle_offsets[key as usize] as usize;
            let end = handle_offsets[key as usize + 1] as usize;
            &handles[start..end]
        } else {
            let offset = key as usize * 8;
            &handles[offset..offset + 8]
        }
    }

    fn has_newer_version(
        &self,
        key: u64,
        start_ts: u64,
        newer_index_files: &[VectorIndexFile],
    ) -> bool {
        let handle = self.get_handle(key);
        // Check if there is a newer version in this index.
        if key > 0 {
            let versions = self.get_versions();
            let version = versions[key as usize];
            let prev_key = key - 1;
            let prev_handle = self.get_handle(prev_key);
            if prev_handle == handle {
                let prev_version = versions[prev_key as usize];
                debug_assert!(
                    prev_version > version,
                    "prev_version: {}, version: {}",
                    prev_version,
                    version
                );
                if prev_version <= start_ts {
                    return true;
                }
            }
        }
        // Check if there is a newer version in newer indexes.
        newer_index_files.iter().any(|file| {
            // Get the key of the same handle in the newer index.
            file.contains_handle(handle)
        })
    }

    pub async fn search(
        &self,
        target: &[f32],
        count: usize,
        start_ts: u64,
        enable_distance_proj: bool,
        newer_index_files: &[VectorIndexFile],
    ) -> Result<(Vec<VectorItem>, Vec<Vec<u8>>)> {
        if !self.is_loaded() {
            self.load_data().await?;
        }
        let versions = self.get_versions();
        let mut results = vec![];
        thread_local! {
            static DELETED_HANDLES: std::cell::RefCell<Vec<Vec<u8>>> = std::cell::RefCell::new(Vec::new());
        }
        let index = self.index();
        let matches = index
            .filtered_search(target, count, |key| {
                let handle = self.get_handle(key);
                let version = versions[key as usize];
                if version > start_ts {
                    return false;
                }
                // Skip if there is a newer version in this index or newer indexes.
                if self.has_newer_version(key, start_ts, newer_index_files) {
                    return false;
                }
                // This is the latest version to read, if it is mvcc deleted, skip it.
                if self.is_null(key as usize) {
                    // Return the deleted keys for deduplication.
                    DELETED_HANDLES.with_borrow_mut(|deleted_handles| {
                        deleted_handles.push(handle.to_vec());
                    });
                    return false;
                }
                true
            })
            .map_err(|e| Other(e.to_string()))?;
        if enable_distance_proj {
            // In distance proj, TableScan expects a distance column and expects no vector
            // column. Thus there is no need to read vector data.
            for (i, &key) in matches.keys.iter().enumerate() {
                let handle = self.get_handle(key);
                let version = versions[key as usize];
                let item = VectorItem {
                    handle: handle.to_vec(),
                    version,
                    distance: matches.distances[i],
                    value: Vec::new(),
                };
                results.push(item);
            }
        } else {
            for (i, &key) in matches.keys.iter().enumerate() {
                let handle = self.get_handle(key);
                let version = versions[key as usize];
                let mut value = vec![0f32; index.dimensions()];
                index
                    .get(key, &mut value)
                    .map_err(|e| Other(e.to_string()))?;
                let item = VectorItem {
                    handle: handle.to_vec(),
                    version,
                    distance: matches.distances[i],
                    value,
                };
                results.push(item);
            }
        }

        let deleted_handles =
            DELETED_HANDLES.with_borrow_mut(|deleted_handles| std::mem::take(deleted_handles));
        Ok((results, deleted_handles))
    }

    pub fn to_vector_index_file_pb(&self) -> kvenginepb::VectorIndexFile {
        let mut vec_idx_file_pb = kvenginepb::VectorIndexFile::new();
        vec_idx_file_pb.set_id(self.file_id());
        vec_idx_file_pb.set_snap_version(self.snap_version().into_inner());
        vec_idx_file_pb.set_smallest(self.smallest().to_vec());
        vec_idx_file_pb.set_biggest(self.biggest().to_vec());
        vec_idx_file_pb.set_meta_offset(self.meta_offset());
        vec_idx_file_pb
    }

    pub fn get_meta_data(data: &Bytes) -> Result<Bytes> {
        let footer_size = VectorIndexFileFooter::size();
        if data.len() < footer_size {
            return Err(Error::InvalidFileSize);
        }
        let footer_data = &data[data.len() - footer_size..];
        let mut footer = VectorIndexFileFooter::default();
        footer.unmarshal(footer_data);
        if footer.magic_number != MAGIC_NUMBER {
            return Err(Error::InvalidMagicNumber);
        }
        if data.len() < footer.props_size() + footer_size {
            return Err(Error::InvalidFileSize);
        }
        Ok(Bytes::copy_from_slice(
            &data[data.len() - footer_size - footer.props_size()..],
        ))
    }
}

#[derive(Debug)]
pub struct VectorItem {
    pub handle: Vec<u8>,
    pub version: u64,
    pub distance: f32,
    pub value: Vec<f32>,
}

impl VectorItem {
    pub fn get_int_handle(&self) -> i64 {
        self.handle.as_slice().get_i64_le()
    }
}

pub(crate) struct VectorItemsReader {
    schema: Schema,
    vector_col_idx: Option<usize>,
    items: Option<Vec<VectorItem>>,
    idx: usize,
    // The vector item already has handle, version and vector column.
    // If we need to read other columns, we can use the inner reader to read them.
    // The inner reader's schema doesn't contains the vector column.
    inner_reader: Option<Box<dyn ColumnarReader>>,
    vector_index: VectorIndex,
    target: Vec<f32>,
    top_k: usize,
    read_ts: u64,
    start_handle: Option<Vec<u8>>,
    end_handle: Option<Vec<u8>>,
    metric: VectorDistanceMetric,
    // The vector index may no need to read vector data just need distance,
    // enable_distance_proj indicate if we need to read distance directly.
    enable_distance_proj: bool,
}

impl VectorItemsReader {
    pub(crate) fn new(
        schema: Schema,
        vector_index: &VectorIndex,
        target: &[f32],
        top_k: usize,
        read_ts: u64,
        start_handle: Option<&[u8]>,
        end_handle: Option<&[u8]>,
        col_levels: &ColumnarLevels,
        encryption_key: Option<EncryptionKey>,
        metric: VectorDistanceMetric,
        enable_distance_proj: bool,
    ) -> Result<Self> {
        let vector_col_idx = if !enable_distance_proj {
            Some(
                schema
                    .columns
                    .iter()
                    .position(|c| c.get_column_id() == vector_index.col_id)
                    .ok_or_else(|| {
                        Error::Other(format!(
                            "vector column {} not found in schema",
                            vector_index.col_id
                        ))
                    })?,
            )
        } else {
            // There is no vector column at all in distance projection mode.
            None
        };

        if enable_distance_proj {
            // We expect this reader to have the same schema requirements as the
            // VectorDistanceProjector.
            VectorDistanceProjector::validate_schema(&schema)?;
        }

        let inner_reader = Self::build_inner_reader(
            &schema,
            vector_index,
            col_levels,
            encryption_key,
            enable_distance_proj,
        )?;

        Ok(Self {
            schema,
            vector_col_idx,
            items: None,
            idx: 0,
            inner_reader,
            vector_index: vector_index.clone(),
            target: target.to_vec(),
            top_k,
            read_ts,
            start_handle: start_handle.map(|h| h.to_vec()),
            end_handle: end_handle.map(|h| h.to_vec()),
            metric,
            enable_distance_proj,
        })
    }

    /// inner_reader reads rest of the column (i.e. all columns to read except
    /// the vector column).
    /// It could be possible that the inner_reader is None, which means
    /// there is no other columns to read.
    fn build_inner_reader(
        schema: &Schema,
        vector_index: &VectorIndex,
        col_levels: &ColumnarLevels,
        encryption_key: Option<EncryptionKey>,
        enable_distance_proj: bool,
    ) -> Result<Option<Box<dyn ColumnarReader>>> {
        if schema.columns.len() == 1 {
            if enable_distance_proj {
                if schema.columns[0].get_column_id() != VIRTUAL_DISTANCE_COLUMN_ID {
                    return Err(table::Error::Other(format!(
                        "Invalid schema, expect virtual distance column, but got {}",
                        schema.columns[0].get_column_id()
                    )));
                }
            } else if schema.columns[0].get_column_id() != vector_index.col_id {
                return Err(table::Error::Other(format!(
                    "Invalid schema, expect vector column {}, but got {}",
                    vector_index.col_id,
                    schema.columns[0].get_column_id()
                )));
            }
            return Ok(None);
        }
        let mut readers: Vec<Box<dyn ColumnarReader>> = vec![];
        let inner_schema: Schema = if enable_distance_proj {
            schema
                .to_schema_buf()
                .retain_columns(|c| c.get_column_id() != VIRTUAL_DISTANCE_COLUMN_ID)
                .into()
        } else {
            schema
                .to_schema_buf()
                .retain_columns(|c| c.get_column_id() != vector_index.col_id)
                .into()
        };
        if inner_schema.columns.len() + 1 != schema.columns.len() {
            return Err(table::Error::Other(format!(
                "Invalid schema, expect {} columns, but got {}",
                inner_schema.columns.len() + 1,
                schema.columns.len()
            )));
        }

        for columnar_level in &col_levels.levels {
            if columnar_level.level == 2 {
                let concat_reader = ColumnarConcatReader::new(
                    &columnar_level.files,
                    inner_schema.clone(),
                    None,
                    encryption_key.clone(),
                );
                readers.push(Box::new(concat_reader));
            } else {
                for file in &columnar_level.files {
                    if file.get_snap_version().unwrap_or_default() > vector_index.snap_version() {
                        continue;
                    }
                    if !file.has_table(schema.table_id) {
                        continue;
                    }
                    let col_reader = ColumnarTableReader::new(
                        file,
                        inner_schema.clone(),
                        None,
                        encryption_key.clone(),
                    );
                    readers.push(Box::new(col_reader));
                }
            }
        }
        if readers.len() == 1 {
            Ok(Some(readers.pop().unwrap()))
        } else {
            Ok(Some(Box::new(ColumnarMergeReader::new(
                inner_schema.clone(),
                readers,
            ))))
        }
    }

    /// In distance projection, last column is the distance col. We fill it
    /// directly from the index.
    async fn read_in_distance_proj(&mut self, block: &mut Block, limit: usize) -> Result<usize> {
        let dis_idx = block.columns.len() - 1;
        let mut col = block.columns.remove(dis_idx);
        let old_idx = self.idx;
        let items = self.items.as_ref().unwrap();

        for _ in 0..limit {
            if self.idx >= items.len() {
                break;
            }

            let item = &items[self.idx];
            // Vector index produce L2 squared distance instead of L2 distance
            let data_f64 = if self.metric == VectorDistanceMetric::L2 {
                item.distance.sqrt() as f64
            } else {
                item.distance as f64
            };
            let data_bytes = data_f64.to_le_bytes();
            col.push_value(&data_bytes);

            if let Some(inner) = &mut self.inner_reader {
                inner.seek(&item.handle).await?;
                inner.read(block, 1).await?;
            } else {
                block.handles.push_value(&item.handle);
                block.versions.push_version(item.version, false);
            }

            self.idx += 1;
        }

        block.columns.insert(dis_idx, col);
        Ok(self.idx - old_idx)
    }

    async fn read_without_distance_proj(
        &mut self,
        block: &mut Block,
        limit: usize,
    ) -> Result<usize> {
        let mut vector_col = block.columns.remove(self.vector_col_idx.unwrap());
        let mut vec_val_buf = vec![];
        let old_idx = self.idx;
        let items = self.items.as_ref().unwrap();
        for _ in 0..limit {
            if self.idx >= items.len() {
                break;
            }
            let item = &items[self.idx];
            let data = VectorFloat32Ref::from_f32(&item.value);
            vec_val_buf.truncate(0);
            vec_val_buf.write_vector_float32(data).unwrap();
            vector_col.push_value(&vec_val_buf);
            if let Some(inner) = &mut self.inner_reader {
                inner.seek(&item.handle).await?;
                inner.read(block, 1).await?;
            } else {
                block.handles.push_value(&item.handle);
                block.versions.push_version(item.version, false);
            }
            self.idx += 1;
        }
        block
            .columns
            .insert(self.vector_col_idx.unwrap(), vector_col);
        Ok(self.idx - old_idx)
    }
}

#[async_trait]
impl ColumnarReader for VectorItemsReader {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    async fn seek(&mut self, mut handle: &[u8]) -> Result<()> {
        let items = self
            .vector_index
            .search(
                &self.target,
                self.top_k,
                self.read_ts,
                self.start_handle.as_deref(),
                self.end_handle.as_deref(),
                self.schema.is_common_handle(),
                self.enable_distance_proj,
            )
            .await?;
        self.idx = if get_fixed_size(&self.schema.handle_column) > 0 {
            let int_handle = handle.get_i64_le();
            search(items.len(), |i| {
                items[i].handle.as_slice().get_i64_le() >= int_handle
            })
        } else {
            search(items.len(), |i| items[i].handle.as_slice() >= handle)
        };
        self.items = Some(items);
        Ok(())
    }

    async fn read(&mut self, block: &mut Block, limit: usize) -> Result<usize> {
        if self.enable_distance_proj {
            self.read_in_distance_proj(block, limit).await
        } else {
            self.read_without_distance_proj(block, limit).await
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct VectorIndexBuildOptions {
    // When the size of the non-indexed vector data exceed this value,
    // a new vector index file is build.
    pub delta_size: usize,
    // When the file count of a vector index exceed this value, a new vector index is rebuild to
    // replace the old vector index files.
    pub rebuild_file_count: usize,

    // When the vector index file size exceed this value, a new vector index file will be built.
    pub max_file_size: usize,
}

impl Default for VectorIndexBuildOptions {
    fn default() -> Self {
        VectorIndexBuildOptions {
            delta_size: 16 * 1024 * 1024,
            rebuild_file_count: 4,
            max_file_size: 2 * 1024 * 1024 * 1024, // 2GB
        }
    }
}

pub struct VectorIndexBuilder {
    index: usearch::Index,
    int_handles: Vec<i64>,
    common_handles: Vec<Vec<u8>>,
    versions: Vec<u64>,
    nulls: Vec<u8>,
    footer: VectorIndexFileFooter,
    is_common_handle: bool,
    metric_repr: i32,
    snap_version: SnapVersion,
    table_id: i64,
    index_id: i32,
    col_id: i32,
    num_rows: u64,
    dimension: usize,
    smallest_int_handle: Option<i64>,
    biggest_int_handle: i64,
    smallest_common_handle: Vec<u8>,
    biggest_common_handle: Vec<u8>,
    handle_size: usize,
    pub(crate) smallest: Vec<u8>,
    pub(crate) biggest: Vec<u8>,
    pub(crate) meta_offset: u32,
}

impl VectorIndexBuilder {
    pub fn new(
        dimension: usize,
        metric: &str,
        snap_version: SnapVersion,
        table_id: i64,
        index_id: i64,
        col_id: i64,
        is_common_handle: bool,
    ) -> Result<Self> {
        let mut opts = new_index_opts();
        opts.dimensions = dimension;
        opts.metric = match metric {
            tidb_query_datatype::VECTOR_INDEX_SPEC_KEY_DISTANCE_METRIC_VAL_L2 => {
                usearch::MetricKind::L2sq
            }
            tidb_query_datatype::VECTOR_INDEX_SPEC_KEY_DISTANCE_METRIC_VAL_COSINE => {
                usearch::MetricKind::Cos
            }
            _ => usearch::MetricKind::Cos,
        };
        let mut footer = VectorIndexFileFooter::default();
        footer.magic_number = MAGIC_NUMBER;
        let index = usearch::Index::new(&opts).map_err(|e| Other(e.to_string()))?;
        Ok(VectorIndexBuilder {
            index,
            int_handles: vec![],
            common_handles: vec![],
            versions: vec![],
            nulls: vec![],
            footer,
            snap_version,
            table_id,
            index_id: index_id as i32,
            col_id: col_id as i32,
            is_common_handle,
            metric_repr: opts.metric.repr,
            num_rows: 0,
            dimension,
            smallest_int_handle: None,
            biggest_int_handle: i64::MIN,
            smallest_common_handle: vec![],
            biggest_common_handle: vec![],
            smallest: vec![],
            biggest: vec![],
            meta_offset: 0,
            handle_size: 0,
        })
    }

    pub fn add_block(&mut self, block: &Block, vec_col_off: usize) -> table::Result<()> {
        let vec_column = &block.get_columns()[vec_col_off];
        self.index
            .reserve(self.index.size() + vec_column.length())
            .unwrap();
        for i in 0..vec_column.length() {
            let vec_val = vec_column.get_value(i);
            // NOTE: If the vector column is the new added column with NOT NULL constraint,
            // the vector value is empty for the existing rows. It's ok to skip these rows.
            // See `decode_row_columns` in `columnar/reader.rs` for more details.
            let need_push = vec_val.is_some() && !vec_val.as_ref().unwrap().is_empty();
            if self.is_common_handle {
                let common_handle = block.handles.get_not_null_value(i);
                if i == 0 || i == vec_column.length() - 1 {
                    self.update_common_handle(common_handle);
                }
                if need_push {
                    self.common_handles.push(common_handle.to_vec());
                    self.handle_size += common_handle.len();
                }
            } else {
                let handle = block.handles.get_int_handle_value(i);
                if i == 0 || i == vec_column.length() - 1 {
                    self.update_int_handle(handle);
                }
                if need_push {
                    self.int_handles.push(handle);
                    self.handle_size += 8;
                }
            }

            if need_push {
                let vec_val = vec_val.as_ref().unwrap();
                let version = block.versions.get_version(i);
                let is_deleted = block.versions.is_null(i);
                if is_deleted {
                    let zeros = vec![0f32; self.dimension];
                    self.index
                        .add(self.num_rows, &zeros)
                        .map_err(|e| Other(e.to_string()))?;
                    self.push_version(version, true);
                } else {
                    let vec_f32: &[f32] = bytemuck::cast_slice(&vec_val[4..]);
                    self.index
                        .add(self.num_rows, vec_f32)
                        .map_err(|e| Other(e.to_string()))?;
                    self.push_version(version, false);
                }
                self.num_rows += 1;
            }
        }
        Ok(())
    }

    fn push_version(&mut self, version: u64, is_deleted: bool) {
        self.versions.push(version);
        self.nulls.push(is_deleted as u8);
    }

    fn update_int_handle(&mut self, handle: i64) {
        if self.smallest_int_handle.is_none() {
            self.smallest_int_handle = Some(handle);
        }
        self.biggest_int_handle = handle;
    }

    fn update_common_handle(&mut self, common_handle: &[u8]) {
        if self.smallest_common_handle.is_empty() {
            self.smallest_common_handle = common_handle.to_vec();
        }
        self.biggest_common_handle.truncate(0);
        self.biggest_common_handle.extend_from_slice(common_handle);
    }

    fn format_version(&self) -> u32 {
        self.footer.format_ver
    }

    pub fn entry_count(&self) -> usize {
        self.num_rows as usize
    }

    /// Estimated the size of the vector index file.
    pub fn estimated_size(&self) -> usize {
        let entry_count = self.num_rows as usize;
        if entry_count == 0 {
            return 0;
        }
        let versions_size = entry_count * 8;
        // NOTE: `serialized_length` will scan all the items in the index, we should not
        // call it too frequently.
        let mut data_size = self.index.serialized_length() + versions_size + self.handle_size;
        if self.format_version() == FORMAT_VERSION_V2 {
            data_size += self.nulls.len();
        }
        data_size
    }

    pub fn build(&mut self) -> Result<Vec<u8>> {
        let entry_count = self.num_rows as usize;
        let versions_size = entry_count * 8;
        let index_size = self.index.serialized_length();
        self.footer.index_size = index_size as u32;
        if self.is_common_handle {
            self.smallest
                .extend_from_slice(&encode_common_handle_row_key(
                    self.table_id,
                    self.smallest_common_handle.as_slice(),
                ));
            self.biggest
                .extend_from_slice(&encode_common_handle_row_key(
                    self.table_id,
                    self.biggest_common_handle.as_slice(),
                ));
        } else {
            self.smallest.extend_from_slice(&encode_row_key(
                self.table_id,
                self.smallest_int_handle.unwrap(),
            ));
            self.biggest
                .extend_from_slice(&encode_row_key(self.table_id, self.biggest_int_handle));
        }
        let mut props_buf = vec![];
        let mut write_prop = |key: &str, val: &[u8]| {
            props_buf.put_u16_le(key.len() as u16);
            props_buf.extend_from_slice(key.as_bytes());
            props_buf.put_u32_le(val.len() as u32);
            props_buf.extend_from_slice(val);
        };
        let index_size_with_align = (index_size + 7) & !7; // align to 8 bytes
        let handles_size = if self.is_common_handle {
            self.common_handles
                .iter()
                .map(|h| h.len())
                .sum::<usize>() /* common handle */
                + (entry_count + 1) * 4 /* handle offsets */
        } else {
            entry_count * 8
        };

        let mut data_size = (index_size_with_align + versions_size + handles_size) as u32;
        if self.format_version() == FORMAT_VERSION_V2 {
            data_size += self.nulls.len() as u32;
        }
        self.meta_offset = data_size;
        write_prop(
            PROP_SNAP_VERSION,
            &self.snap_version.into_inner().to_le_bytes(),
        );
        write_prop(PROP_TABLE_ID, &self.table_id.to_le_bytes());
        write_prop(PROP_INDEX_ID, &self.index_id.to_le_bytes());
        write_prop(PROP_COLUMN_ID, &self.col_id.to_le_bytes());
        write_prop(PROP_IS_COMMON_HANDLE, &[self.is_common_handle as u8]);
        write_prop(PROP_METRIC_REPR, &self.metric_repr.to_le_bytes());
        write_prop(PROP_SMALLEST, &self.smallest);
        write_prop(PROP_BIGGEST, &self.biggest);
        self.footer.props_size = props_buf.len() as u32;
        let footer_data = self.footer.marshal();

        let mut buf = Vec::with_capacity(data_size as usize + props_buf.len() + footer_data.len());
        buf.resize(index_size_with_align, 0);
        self.index
            .save_to_buffer(&mut buf)
            .map_err(|e| Other(e.to_string()))?;
        buf.extend_from_slice(bytemuck::cast_slice(&self.versions));
        if self.is_common_handle {
            let mut handle_offsets = Vec::with_capacity(entry_count + 1);
            handle_offsets.push(0);
            let total_handle_size = handles_size - (entry_count + 1) * 4;
            let mut handle_data = Vec::with_capacity(total_handle_size);
            for i in 0..entry_count {
                handle_data.extend_from_slice(&self.common_handles[i]);
                handle_offsets.push(handle_data.len() as u32);
            }
            buf.extend_from_slice(bytemuck::cast_slice(&handle_offsets));
            buf.extend_from_slice(&handle_data);
        } else {
            buf.extend_from_slice(bytemuck::cast_slice(&self.int_handles));
        }
        if self.format_version() == FORMAT_VERSION_V2 {
            buf.extend_from_slice(&self.nulls);
        }
        buf.extend_from_slice(&props_buf);
        buf.extend_from_slice(&footer_data);
        Ok(buf)
    }
}

const PROP_SMALLEST: &str = "smallest";
const PROP_BIGGEST: &str = "biggest";
const PROP_SNAP_VERSION: &str = "snap_ver";
const PROP_TABLE_ID: &str = "tbl_id";
const PROP_INDEX_ID: &str = "idx_id";
const PROP_COLUMN_ID: &str = "col_id";
const PROP_IS_COMMON_HANDLE: &str = "c_h";
const PROP_METRIC_REPR: &str = "m_r";

fn new_index_opts() -> IndexOptions {
    let mut opts = IndexOptions::default();
    opts.connectivity = CONNECTIVITY;
    opts.expansion_add = EXPANSION_ADD;
    opts.expansion_search = EXPANSION_SEARCH;
    opts.quantization = QUANTIZATION;
    opts
}

#[cfg(test)]
mod tests {
    use std::{convert::TryInto, fs, sync::Arc};

    use bstr::ByteSlice;
    use futures::executor::block_on;
    use schema::schema::StorageClassSpec;
    use tidb_query_datatype::{
        codec::{
            data_type::VectorFloat32,
            mysql::VectorFloat32Encoder,
            table::{encode_common_handle_row_key, encode_row_key},
        },
        FieldTypeAccessor, FieldTypeTp,
    };
    use tipb::ColumnInfo;

    use crate::table::{
        columnar::{
            new_common_handle_column_info, new_int_handle_column_info, new_version_column_info,
            Block,
        },
        file::LocalFile,
        schema_file::{Schema, SchemaBuf},
        vector_index::{VectorIndex, VectorIndexBuilder, VectorIndexFile},
        SnapVersion,
    };

    const TEST_DIMENSION: usize = 3;

    #[test]
    fn test_vector_index_file() {
        ::test_util::init_log_for_test();
        for common_handle in [true, false] {
            let vec_idx = build_vector_index_file(common_handle, 100, 200, 1.into());
            block_on(vec_idx.load_data()).unwrap();
            assert_eq!(vec_idx.index().size(), 100);
            assert_eq!(vec_idx.table_id, 1);
            assert_eq!(vec_idx.index_id, 1);
            assert_eq!(vec_idx.column_id, 1);
            assert_eq!(vec_idx.snap_version.into_inner(), 1);
            assert_eq!(vec_idx.is_common_handle, common_handle);
            if common_handle {
                assert_eq!(
                    vec_idx.smallest.as_bytes(),
                    encode_common_handle_row_key(1, &i_to_common_handle(100)).as_slice()
                );
                assert_eq!(
                    vec_idx.biggest.as_bytes(),
                    encode_common_handle_row_key(1, &i_to_common_handle(199)).as_slice()
                );
            } else {
                assert_eq!(
                    vec_idx.smallest.as_bytes(),
                    encode_row_key(1, 100).as_slice()
                );
                assert_eq!(
                    vec_idx.biggest.as_bytes(),
                    encode_row_key(1, 199).as_slice()
                );
            }
            for key in 0u64..100 {
                let mut vec_val = vec![0f32; TEST_DIMENSION];
                let cnt = vec_idx.index().get(key, &mut vec_val).unwrap();
                assert_eq!(cnt, 1);
                if common_handle {
                    assert_eq!(
                        vec_idx.get_handle(key),
                        i_to_common_handle(100 + key as i64)
                    );
                } else {
                    assert_eq!(vec_idx.get_handle(key), &(100 + key as i64).to_le_bytes());
                }
            }
            let (items, _) =
                block_on(vec_idx.search(&[50.0f32, 150.0f32, 250.0f32], 3, u64::MAX, false, &[]))
                    .unwrap();
            assert_eq!(items.len(), 3);
            assert_eq!(items[0].value, vec![50.0f32, 150.0f32, 250.0f32]);
            assert_eq!(items[1].value, vec![51.0f32, 151.0f32, 251.0f32]);
            assert_eq!(items[2].value, vec![49.0f32, 149.0f32, 249.0f32]);
        }
    }

    fn i_to_common_handle(i: i64) -> Vec<u8> {
        format!("{:08x}", i).as_bytes().to_vec()
    }

    fn build_vector_index_file(
        common_handle: bool,
        start: i64,
        end: i64,
        snap_version: SnapVersion,
    ) -> VectorIndexFile {
        build_vector_index_file_with_deleted(common_handle, start, end, 0, 0, snap_version)
    }

    #[test]
    fn test_vector_index_deduplication() {
        // Create multiple vector index files with duplicate handles
        let mut vector_index = VectorIndex::new(1, 1, 1);
        vector_index
            .files
            .push(build_vector_index_file(false, 100, 200, 1.into()));
        vector_index
            .files
            .push(build_vector_index_file(false, 150, 200, 2.into()));
        vector_index
            .files
            .push(build_vector_index_file(false, 151, 200, 3.into()));
        vector_index.sort();

        // Perform a search
        let query_vector = vec![50.0f32, 150.0f32, 250.0f32];
        let mut items =
            block_on(vector_index.search(&query_vector, 3, u64::MAX, None, None, false, false))
                .unwrap();

        // each file returns 3, total is 9, truncated to 3.
        assert_eq!(items.len(), 3);
        items.sort_by(|a, b| a.distance.total_cmp(&b.distance));
        assert_eq!(items[0].value, vec![50.0f32, 150.0f32, 250.0f32]);
        assert_eq!(items[0].version, 2);
        assert_eq!(items[1].value, vec![51.0f32, 151.0f32, 251.0f32]);
        assert_eq!(items[1].version, 3);
        assert_eq!(items[2].value, vec![49.0f32, 149.0f32, 249.0f32]);
        assert_eq!(items[2].version, 1);
    }

    fn build_vector_index_file_with_deleted(
        common_handle: bool,
        start: i64,
        end: i64,
        del_start: i64,
        del_end: i64,
        snap_version: SnapVersion,
    ) -> VectorIndexFile {
        let mut builder =
            VectorIndexBuilder::new(3, "cosine", snap_version, 1, 1, 1, common_handle).unwrap();
        let handle_column = if common_handle {
            new_common_handle_column_info()
        } else {
            new_int_handle_column_info()
        };
        let version_column = new_version_column_info();
        let mut vec_col_info = ColumnInfo::new();
        vec_col_info.set_column_id(1);
        vec_col_info.set_tp(FieldTypeTp::TiDbVectorFloat32 as i32);
        vec_col_info.set_flen(TEST_DIMENSION as isize);
        let columns = vec![vec_col_info];
        let schema_buf = SchemaBuf::new(
            1,
            handle_column,
            version_column,
            columns,
            vec![],
            0,
            vec![],
            vec![],
            StorageClassSpec::default(),
            None,
        );
        let schema = Schema::new(schema_buf);
        let mut block = Block::new(&schema);
        for i in start..end {
            if i >= del_start && i < del_end {
                if common_handle {
                    block.handles.push_value(&i_to_common_handle(i));
                } else {
                    block.handles.push_value(&i.to_le_bytes());
                }
                block
                    .versions
                    .push_version(snap_version.into_inner() + 1, true);
            }
            if common_handle {
                block.handles.push_value(&i_to_common_handle(i));
            } else {
                block.handles.push_value(&i.to_le_bytes());
            }
            block
                .versions
                .push_version(snap_version.into_inner(), false);
        }
        let vec_col_buf = &mut block.columns[0];
        for i in start..end {
            if i >= del_start && i < del_end {
                let vec_f32_val = vec![0f32; TEST_DIMENSION];
                let vec_f32 = VectorFloat32::copy_from_f32(&vec_f32_val);
                let mut buf = vec![];
                buf.write_vector_float32(vec_f32.as_ref()).unwrap();
                vec_col_buf.push_value(&buf);
            }
            let vec_f32 =
                VectorFloat32::copy_from_f32(&[(i - 100) as f32, i as f32, (i + 100) as f32]);
            let mut buf = vec![];
            buf.write_vector_float32(vec_f32.as_ref()).unwrap();
            vec_col_buf.push_value(&buf);
        }
        builder.add_block(&block, 0).unwrap();
        let data = builder.build().unwrap();
        let temp_path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
        fs::write(&temp_path, data).unwrap();
        let local_file = LocalFile::open(1, temp_path.to_path_buf()).unwrap();
        VectorIndexFile::new(Arc::new(local_file), 0, None).unwrap()
    }

    #[test]
    fn test_vector_index_mvcc_delete() {
        ::test_util::init_log_for_test();
        {
            // Test one vector index file with mvcc delete.
            let mut vector_index = VectorIndex::new(1, 1, 1);
            vector_index
                .files
                .push(build_vector_index_file_with_deleted(
                    false,
                    100,
                    200,
                    150,
                    160,
                    1.into(),
                ));
            vector_index.sort();

            let query_vector = vec![50.0f32, 150.0f32, 250.0f32];
            let items = block_on(vector_index.search(
                &query_vector,
                100,
                u64::MAX,
                None,
                None,
                false,
                false,
            ))
            .unwrap();

            for item in items {
                let handle = item.handle;
                let int_handle = i64::from_le_bytes(handle.try_into().unwrap());
                assert!(!(150..160).contains(&int_handle));
            }
        }

        {
            // Test multiple vector index files with mvcc delete.
            let mut vector_index = VectorIndex::new(1, 1, 1);
            vector_index
                .files
                .push(build_vector_index_file(false, 100, 200, 1.into()));
            vector_index
                .files
                .push(build_vector_index_file(false, 150, 200, 2.into()));
            vector_index
                .files
                .push(build_vector_index_file(false, 151, 200, 3.into()));
            vector_index
                .files
                .push(build_vector_index_file_with_deleted(
                    false,
                    140,
                    160,
                    150,
                    170,
                    4.into(),
                ));
            vector_index.sort();

            let query_vector = vec![50.0f32, 150.0f32, 250.0f32];
            let items = block_on(vector_index.search(
                &query_vector,
                100,
                u64::MAX,
                None,
                None,
                false,
                false,
            ))
            .unwrap();
            for item in items {
                let handle = item.handle;
                let int_handle = i64::from_le_bytes(handle.try_into().unwrap());
                assert!(!(150..160).contains(&int_handle));
            }
        }
    }
}

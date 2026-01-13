// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use async_trait::async_trait;
use tidb_query_datatype::{
    FieldTypeTp,
    codec::mysql::{VectorFloat32Decoder, VectorFloat32Ref},
};
use tipb::{AnnQueryInfo, ColumnInfo};

use crate::{
    ia::types::FileSegmentIdent,
    table::{
        Error, Result,
        columnar::{Block, ColumnBuffer, ColumnarReader},
        schema_file::{Schema, SchemaBuf},
    },
};

pub const VIRTUAL_DISTANCE_COLUMN_ID: i64 = -2000;

type DistanceFn = for<'a, 'b> fn(
    VectorFloat32Ref<'a>,
    VectorFloat32Ref<'b>,
) -> tidb_query_datatype::codec::Result<f64>;

/// A reader wrapper takes vector data column from the inner reader and produce
/// a vector distance column. The distance column will take place of the vector
/// data column.
///
/// This is used when distance projection is enabled and index is not
/// built yet. In such cases TiDB always expect us to return a distance column
/// so that we must calculate one by using this struct.
///
/// # Errors
///
/// The last column of the schema must be the distance projection column.
/// The inner reader must have a schema that last column matches the vector
/// column in the `AnnQueryInfo`. Otherwise errors will be returned.
pub struct VectorDistanceProjector {
    inner: Box<dyn ColumnarReader>,
    schema: Schema,
    ann_query: Arc<AnnQueryInfo>,

    /// Pre-populated from ann_query
    distance_fn: DistanceFn,
}

impl VectorDistanceProjector {
    /// When vector search distance proj is enabled, TableScan's schema contains
    /// a VirtualDistance column at the end, and does not contain the vector
    /// column. However, when we are reading from data that vector index is
    /// not built yet, we need to read out the vector column in order
    /// to calculate the distance column that TableScan asks for. So this
    /// function builds the schema for that case.
    ///
    /// The schema produced by this function will have the vector column
    /// at the end, and the VirtualDistance column will be removed.
    pub fn generate_inner_schema(schema: &Schema, ann_query: &Arc<AnnQueryInfo>) -> Result<Schema> {
        Self::validate_schema(schema)?;

        // Also verify whether ann_query.column is valid. We will use ann_query.column
        // to reconstruct the vector column for reading.
        if !ann_query.get_column().has_column_id() {
            return Err(Error::Other(
                "missing ann_query.column.column_id for distance proj".into(),
            ));
        }
        if ann_query.get_column().get_tp() != FieldTypeTp::TiDbVectorFloat32 as i32 {
            return Err(Error::Other(format!(
                "ann_query.column.tp must be TiDbVectorFloat32 for distance proj, but got {}",
                ann_query.get_column().get_tp()
            )));
        }

        let mut new_col = ColumnInfo::new();
        new_col.set_column_id(ann_query.get_column().get_column_id());
        new_col.set_tp(ann_query.get_column().get_tp());
        new_col.set_flag(ann_query.get_column().get_flag());

        let mut new_inner = (*schema.inner).clone();
        new_inner.columns.pop().unwrap();
        new_inner.columns.push(new_col);

        let buf = SchemaBuf {
            inner: Arc::new(new_inner),
            partitions: schema.partitions.clone(),
            table_id: schema.table_id,
            sc_spec: schema.sc_spec.clone(),
            is_sub_partition: schema.is_sub_partition,
        };
        Ok(Schema::new(buf))
    }

    /// Checks whether this schema is a valid schema for distance projection to
    /// produce, which is: the last column of the schema must be the virtual
    /// distance column (-2000).
    pub fn validate_schema(schema: &Schema) -> Result<()> {
        let last_col = schema
            .columns
            .last()
            .ok_or_else(|| Error::Other("schema must not be empty for distance proj".into()))?;
        // check column_id if is equal to -2000
        if last_col.get_column_id() != VIRTUAL_DISTANCE_COLUMN_ID {
            return Err(Error::Other(format!(
                "schema last column id must be -2000 for distance proj, but got {}",
                last_col.get_column_id()
            )));
        }
        // check column_id if is float32
        if last_col.get_tp() != FieldTypeTp::Float as i32 {
            return Err(Error::Other(format!(
                "schema last column type must be float32 for distance proj, but got {}",
                last_col.get_tp()
            )));
        }
        // distance column must be nullable
        if last_col.get_flag() as u32 != 0 {
            return Err(Error::Other(format!(
                "schema last column type must be nullable for distance proj, but got {}",
                last_col.get_tp()
            )));
        }
        Ok(())
    }

    /// inner must have a schema that matches `generate_inner_schema`.
    pub fn new(
        inner: Box<dyn ColumnarReader>,
        schema_with_distance: Schema,
        ann_query: Arc<AnnQueryInfo>,
    ) -> Result<Self> {
        // Validate outer schema
        Self::validate_schema(&schema_with_distance)?;
        // Validate inner schema matches what's in the ann_query
        let last_inner_col = inner.schema().columns.last().ok_or_else(|| {
            Error::Other("inner schema must not be empty for distance proj".into())
        })?;
        if last_inner_col.get_column_id() != ann_query.get_column().get_column_id() {
            return Err(Error::Other(format!(
                "inner schema last column id {} does not match ann_query which is {}",
                last_inner_col.get_column_id(),
                ann_query.get_column().get_column_id()
            )));
        }

        let distance_fn: DistanceFn = match ann_query.get_distance_metric() {
            tipb::VectorDistanceMetric::Cosine => |a, b| a.cosine_distance(b),
            tipb::VectorDistanceMetric::L1 => |a, b| a.l1_distance(b),
            tipb::VectorDistanceMetric::L2 => |a, b| a.l2_distance(b),
            tipb::VectorDistanceMetric::InnerProduct => |a, b| a.inner_product(b),
            _ => {
                return Err(Error::Other(format!(
                    "unsupported distance metric: {:?}",
                    ann_query.get_distance_metric()
                )));
            }
        };

        Ok(Self {
            inner,
            schema: schema_with_distance,
            ann_query,
            distance_fn,
        })
    }
}

#[async_trait]
impl ColumnarReader for VectorDistanceProjector {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    async fn seek(&mut self, handle: &[u8]) -> crate::table::Result<()> {
        self.inner.seek(handle).await
    }

    /// VectorDistanceReader performs a read by delegating to its inner reader
    /// and transforms the vector column into a distance column in place.
    ///
    /// Read flow:
    /// VectorDistanceReader::read
    ///     -> inner.read (fetch vector data)
    ///     -> transform vector -> compute distance
    ///     -> replace vector column with distance column
    async fn read(&mut self, block: &mut Block, limit: usize) -> crate::table::Result<usize> {
        // Perform a quick check for the block schema
        if block
            .columns
            .last()
            .is_none_or(|c| c.col_id != VIRTUAL_DISTANCE_COLUMN_ID as i32)
        {
            return Err(Error::Other(
                "block schema does not match VectorDistanceProjector schema".into(),
            ));
        }

        let mut inner_block = Block::new(self.inner.schema());
        let vec_col_index = inner_block.columns.len() - 1;
        let total_read_row = self.inner.read(&mut inner_block, limit).await?;
        block.handles = inner_block.handles;
        block.versions = inner_block.versions;
        block.columns = inner_block.columns;

        let mut dis_col = ColumnBuffer::new_from_col_info(self.schema().columns.last().unwrap());

        let mut ref_vec_bytes = self.ann_query.get_ref_vec_f32();
        let ref_vec = ref_vec_bytes
            .read_vector_float32_ref()
            .map_err(|e| Error::Other(format!("invalid ann_query_info ref_vec: {}", e)))?;
        let ref_vec = ref_vec
            .to_aligned_ref()
            .map_err(|e| Error::Other(format!("invalid ann_query_info ref_vec: {}", e)))?;
        let vec_col = &block.columns[vec_col_index];

        // dis_col[x]=distance_fn(ref_vec, vec_col[x])
        for row in 0..total_read_row {
            if vec_col.is_nullable() && vec_col.is_null(row) {
                dis_col.push_null();
                continue;
            }

            // The distance column is a float32 column. In the tikv columnar path, it is
            // fixed_sized and is uniformly stored and read using the f64 type(8
            // byte), so we can just insert f64 type data.
            let mut data = vec_col.get_value(row).unwrap();
            let data_vec = data
                .read_vector_float32_ref()
                .map_err(|e| Error::Other(format!("invalid data vec: {}", e)))?;
            let data_vec = data_vec
                .to_aligned_ref()
                .map_err(|e| Error::Other(format!("invalid data vec: {}", e)))?;

            let distance: f64 = (self.distance_fn)(data_vec, ref_vec)
                .map_err(|e| Error::Other(format!("{}", e)))?;
            dis_col.push_value(&distance.to_le_bytes());
        }

        // Replace the last column (vec column) to be the distance column
        block.columns.pop().unwrap();
        block.columns.push(dis_col);

        Ok(total_read_row)
    }

    fn get_remote_segments(&self) -> crate::table::Result<(Vec<FileSegmentIdent>, usize)> {
        self.inner.get_remote_segments()
    }
}

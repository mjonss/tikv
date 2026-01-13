// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{fmt, result::Result as StdResult, sync::Arc, time::Duration as StdDuration};

use bytes::{Buf, BufMut};
use serde_repr::{Deserialize_repr, Serialize_repr};
use tidb_query_datatype::{
    Collation, FieldTypeFlag, FieldTypeTp,
    codec::{
        Datum, datum,
        mysql::{Decimal, Duration, Enum, Set, Time, TimeType, VectorFloat32},
    },
    expr::EvalContext,
};
use tikv_util::{
    box_err, box_try,
    buffer_vec::BufferVec,
    codec::number::{U8_SIZE, U32_SIZE, U64_SIZE},
    warn,
};

pub type Error = Box<dyn std::error::Error + Sync + Send>;
pub type Result<T> = StdResult<T, Error>;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DbInfo {
    #[serde(rename = "id")]
    pub id: i64,
    #[serde(rename = "db_name")]
    pub name: CiStr,
    pub charset: String,
    pub collate: String,
    #[serde(skip)]
    pub tables: Vec<TableInfo>,
    pub state: SchemaState,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CiStr {
    #[serde(rename = "O")]
    pub o: String,
    #[serde(rename = "L")]
    pub l: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct StorageClassTransitionInfo {
    pub tier: String,
    pub after_days: u32,
    pub after_seconds: u32,
}

pub const STATE_PUBLIC: SchemaState = SchemaState(5);

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TableInfo {
    pub id: i64,
    pub name: CiStr,
    pub charset: String,
    pub collate: String,
    pub cols: Option<Vec<ColumnInfo>>,
    pub max_col_id: i64,
    pub index_info: Option<Vec<IndexInfo>>,
    pub state: SchemaState,
    pub pk_is_handle: bool,
    pub is_common_handle: bool,
    pub common_handle_version: u16,
    pub comment: String,
    pub partition: Option<PartitionInfo>,
    pub version: u16,
    pub is_columnar: bool,
    pub tiflash_replica: Option<TiFlashReplica>,
    // `None`: the storage class is never set.
    pub storage_class_tier: Option<String>,
    pub storage_class_transitions: Option<Vec<StorageClassTransitionInfo>>,
}

impl TableInfo {
    #[inline]
    pub fn with_required_changes(&self) -> bool {
        self.with_columnar() || self.with_storage_class_spec()
    }

    pub fn with_columnar(&self) -> bool {
        self.cols.as_ref().is_some_and(|cols| !cols.is_empty()) && self.build_columnar()
    }

    fn build_columnar(&self) -> bool {
        if self.comment.contains("columnar_engine") {
            return true;
        }
        if let Some(tiflash_replica) = &self.tiflash_replica {
            if tiflash_replica.count > 0 {
                return true;
            }
        }
        false
    }

    pub fn with_storage_class_spec(&self) -> bool {
        self.storage_class_spec().is_specified() || self.partition_with_storage_class_spec()
    }

    fn partition_with_storage_class_spec(&self) -> bool {
        self.partition
            .as_ref()
            .is_some_and(|p| p.definitions.iter().any(|d| d.with_storage_class_spec()))
    }

    pub fn storage_class_spec(&self) -> StorageClassSpec {
        StorageClassSpec::from_tidb(
            self.storage_class_tier.as_deref(),
            self.storage_class_transitions.as_deref(),
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TiFlashReplica {
    #[serde(rename = "Count")]
    pub count: u64,
    #[serde(rename = "LocationLabels")]
    pub location_labels: Option<Vec<String>>,
    #[serde(rename = "Available")]
    pub available: bool,
    #[serde(rename = "AvailablePartitionIDs")]
    pub available_partition_ids: Option<Vec<i64>>,
}

// ColumnInfo provides meta data describing of a table column.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub id: i64,
    pub name: CiStr,
    pub offset: i64,
    pub origin_default: Option<String>,
    pub origin_default_bit: Option<String>, // base64 encoded string.
    pub default: Option<String>,
    pub default_bit: Option<String>, // base64 encoded string.
    // DefaultIsExpr is indicates the default value string is expr.
    pub default_is_expr: bool,
    pub generated_expr_string: String,
    pub generated_stored: bool,
    #[serde(rename = "type")]
    pub field_type: FieldType,
    pub state: SchemaState,
    pub comment: String,
    // A hidden column is used internally(expression index) and are not accessible by users.
    pub hidden: bool,
    // Version means the version of the column info.
    // Version = 0: For OriginDefaultValue and DefaultValue of timestamp column will stores the
    // default time in system time zone.              That is a bug if multiple TiDB servers in
    // different system time zone. Version = 1: For OriginDefaultValue and DefaultValue of
    // timestamp column will stores the default time in UTC time zone.              This will
    // fix bug in version 0. For compatibility with version 0, we add version field in column info
    // struct.
    pub version: u64,
    pub vector_index: Option<VectorIndexInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexInfo {
    pub id: i64,
    pub idx_name: CiStr,
    pub tbl_name: CiStr,
    pub idx_cols: Vec<IndexColumn>,
    pub state: SchemaState,
    pub is_unique: bool,
    pub is_primary: bool,
    pub is_invisible: bool,
    pub is_global: bool,
    pub mv_index: Option<bool>,
    pub vector_index: Option<VectorIndexInfo>,
    pub full_text_index: Option<FullTextIndexInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorIndexInfo {
    pub kind: String,
    pub dimension: u64,
    pub distance_metric: String,
}

/// Aligned with TiDB: pkg/parser/model/index_full_text.go
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FullTextIndexInfo {
    pub parser_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexColumn {
    pub name: CiStr,
    pub offset: isize,
    // Length of prefix when using column prefix
    // for indexing;
    // UnspecifedLength if not using prefix indexing
    pub length: isize,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct FieldType {
    pub tp: i32,
    pub flag: i32,
    pub flen: i64,
    pub decimal: i32,
    pub charset: String,
    pub collate: String,
    pub elems: Option<Vec<String>>, // replace `()` with the appropriate type
    pub elems_is_binary_lit: Option<Vec<bool>>, // replace `()` with the appropriate type
    pub array: Option<bool>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SchemaState(u8);

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PartitionType(usize);

// PartitionInfo provides table partition info.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PartitionInfo {
    #[serde(rename = "type")]
    pub type_: PartitionType,
    pub expr: String,
    pub columns: Option<Vec<CiStr>>,
    // User may already create table with partition but table partition is not
    // yet supported back then. When Enable is true, write/read need use tid
    // rather than pid.
    pub enable: bool,
    pub definitions: Vec<PartitionDefinition>,
    // AddingDefinitions is filled when adding a partition that is in the mid state.
    pub adding_definitions: Option<Vec<PartitionDefinition>>,
    // DroppingDefinitions is filled when dropping a partition that is in the mid state.
    pub dropping_definitions: Option<Vec<PartitionDefinition>>,
    pub states: Option<Vec<PartitionState>>,
    pub num: u64,
}

// PartitionState is the state of the partition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionState {
    pub id: i64,
    pub state: SchemaState,
}

// PartitionDefinition defines a single partition.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PartitionDefinition {
    pub id: i64,
    pub name: CiStr,
    pub less_than: Option<Vec<String>>,
    pub in_values: Option<Vec<Vec<String>>>,
    pub comment: Option<String>,
    // `None`: the storage class is never set.
    pub storage_class_tier: Option<String>,
    pub storage_class_transitions: Option<Vec<StorageClassTransitionInfo>>,
}

impl PartitionDefinition {
    pub fn with_storage_class_spec(&self) -> bool {
        self.storage_class_spec().is_specified()
    }

    pub fn storage_class_spec(&self) -> StorageClassSpec {
        StorageClassSpec::from_tidb(
            self.storage_class_tier.as_deref(),
            self.storage_class_transitions.as_deref(),
        )
    }
}

// SchemaDiff contains the schema modification at a particular schema version.
// It is used to reduce schema reload cost.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SchemaDiff {
    pub version: i64,
    #[serde(rename = "type")]
    pub action_type: ActionType,
    pub schema_id: i64,
    pub table_id: i64,
    pub old_table_id: i64,
    pub old_schema_id: i64,
    pub regenerate_schema_map: bool,
    pub affected_opts: Option<Vec<AffectedOption>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ActionType(u8);

// AffectedOption is used when a ddl affects multi tables.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AffectedOption {
    pub schema_id: i64,
    pub table_id: i64,
    pub old_table_id: i64,
    pub old_schema_id: i64,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SchemaVersionResponse {
    pub version: i64,
    pub schemas: Vec<TableInfo>,
}

/// Storage class tier strings which are the same with TiDB.
pub const STORAGE_CLASS_TIER_STANDARD: &str = "STANDARD";
pub const STORAGE_CLASS_TIER_IA: &str = "IA";

pub const STORAGE_CLASS_STR_UNSPECIFIED: &str = "UNSPECIFIED";
pub const STORAGE_CLASS_SPEC_STR_AUTO: &str = "AUTO";

const STORAGE_CLASS_KEY: &str = "_storage_class";

#[derive(PartialEq, Clone, Copy, Default, Serialize_repr, Deserialize_repr)]
#[repr(u8)]
#[serde(rename_all = "kebab-case")]
pub enum StorageClass {
    /// User doesn't specify the storage class.
    ///
    /// Most tables and partitions should be of this. Used to bypass storage
    /// class relevant logic for performance.
    #[default]
    Unspecified = 0,

    /// Standard Tier for general purpose.
    Standard = 1,

    /// Infrequent Access Tier for warm / cold data.
    Ia = 2,
}

impl StorageClass {
    pub fn display(&self) -> &'static str {
        match self {
            Self::Unspecified => STORAGE_CLASS_STR_UNSPECIFIED,
            Self::Standard => STORAGE_CLASS_TIER_STANDARD,
            Self::Ia => STORAGE_CLASS_TIER_IA,
        }
    }

    #[cfg(feature = "testexport")]
    pub fn to_tidb(&self) -> String {
        match self {
            Self::Unspecified | Self::Standard => STORAGE_CLASS_TIER_STANDARD.to_string(),
            Self::Ia => STORAGE_CLASS_TIER_IA.to_string(),
        }
    }

    // For compatibility test.
    #[cfg(test)]
    fn marshal(&self) -> Vec<u8> {
        match self {
            Self::Unspecified => vec![],
            _ => vec![*self as u8],
        }
    }

    // For compatibility test.
    #[cfg(test)]
    fn unmarshal(val: Option<&[u8]>) -> Self {
        match val {
            Some(val) if !val.is_empty() => {
                debug_assert_eq!(val.len(), 1, "invalid val: {:?}", val);
                StorageClass::try_from(val[0]).unwrap_or(StorageClass::Unspecified)
            }
            _ => StorageClass::Unspecified,
        }
    }

    /// User has specified the storage class.
    #[inline]
    pub fn is_specified(&self) -> bool {
        *self != Self::Unspecified
    }

    /// The storage class requires to occupy a region exclusively.
    #[inline]
    fn require_exclusive_region(&self) -> bool {
        matches!(self, Self::Ia)
    }

    #[inline]
    pub fn is_ia(&self) -> bool {
        matches!(self, Self::Ia)
    }

    #[inline]
    pub fn is_sync(&self) -> bool {
        matches!(self, Self::Unspecified | Self::Standard)
    }

    pub fn from_tidb_storage_class_tier(tier: Option<&str>) -> Self {
        let mut sc = tier
            .and_then(|x| x.try_into().map_err(|e| warn!("{:?}", e)).ok())
            .unwrap_or(StorageClass::Unspecified);
        if sc == StorageClass::Standard {
            // We consider `Standard` & `Unspecified` as the same. So do the conversion for
            // easy.
            sc = StorageClass::Unspecified;
        }
        sc
    }
}

impl TryFrom<&str> for StorageClass {
    type Error = String;

    fn try_from(value: &str) -> StdResult<Self, Self::Error> {
        match value {
            "" => Ok(StorageClass::Unspecified),
            STORAGE_CLASS_TIER_STANDARD => Ok(StorageClass::Standard),
            STORAGE_CLASS_TIER_IA => Ok(StorageClass::Ia),
            _ => {
                debug_assert!(false, "Unknown storage class: {}", value);
                Err(format!("Unknown storage class: {}", value))
            }
        }
    }
}

impl TryFrom<u8> for StorageClass {
    type Error = String;

    fn try_from(value: u8) -> StdResult<Self, Self::Error> {
        // `0` is not expected to be passed in.
        match value {
            0 => Ok(StorageClass::Unspecified),
            1 => Ok(StorageClass::Standard),
            2 => Ok(StorageClass::Ia),
            _ => {
                debug_assert!(false, "Unknown storage class: {}", value);
                Err(format!("Unknown storage class: {}", value))
            }
        }
    }
}

impl fmt::Debug for StorageClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}({})", self.display(), *self as u8)
    }
}

impl fmt::Display for StorageClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.display())
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct StorageClassTransitRule {
    pub tier: StorageClass,
    pub transit_after: StdDuration,
}

#[derive(Default, Clone, Debug, PartialEq)]
pub struct StorageClassSpec {
    pub default_tier: StorageClass,
    pub transit_rules: Vec<StorageClassTransitRule>,
}

const STORAGE_CLASS_SPEC_FORMAT_VER: u8 = 1;

impl StorageClassSpec {
    pub fn marshal(&self) -> Vec<u8> {
        if !self.default_tier.is_specified() && self.transit_rules.is_empty() {
            return vec![];
        }

        // Write `default_tier` first for compatibility.
        let marshal_len = if self.transit_rules.is_empty() {
            U8_SIZE /* default_tier */
        } else {
            U8_SIZE /* default_tier */ + U8_SIZE /* ver */ + U32_SIZE /* len */
                + self.transit_rules.len() * (U8_SIZE /* tier */ + U64_SIZE /* duration */)
        };
        let mut buf = Vec::with_capacity(marshal_len);

        // default_tier.
        buf.put_u8(self.default_tier as u8);
        if self.transit_rules.is_empty() {
            return buf;
        }

        // ver.
        buf.put_u8(STORAGE_CLASS_SPEC_FORMAT_VER);

        // transit_rules.
        buf.put_u32_le(self.transit_rules.len() as u32);
        for rule in &self.transit_rules {
            buf.put_u8(rule.tier as u8);
            buf.put_u64_le(rule.transit_after.as_secs());
        }
        buf
    }

    pub fn unmarshal(buf: Option<&[u8]>) -> Self {
        Self::try_unmarshal(buf)
            .map_err(|e| {
                debug_assert!(false, "try unmarshal failed: {:?}, buf: {:?}", e, buf);
                warn!("try unmarshal failed: {:?}", e; "buf" => ?buf);
            })
            .unwrap_or_default()
    }

    pub fn try_unmarshal(buf: Option<&[u8]>) -> Result<Self> {
        let Some(mut buf) = buf else {
            return Ok(Self::default());
        };
        if buf.is_empty() {
            return Ok(Self::default());
        }

        let default_tier = StorageClass::try_from(buf.get_u8())
            .map_err(|e| -> Error { box_err!("invalid default tier: {}", e) })?;
        // For compatibility.
        if !buf.has_remaining() {
            return Ok(Self {
                default_tier,
                transit_rules: vec![],
            });
        }

        let ver = buf.get_u8();
        if ver != STORAGE_CLASS_SPEC_FORMAT_VER {
            return Err(box_err!("invalid version: {}", ver));
        }
        let len = buf.get_u32_le() as usize;
        let mut rules = Vec::with_capacity(len);
        for _ in 0..len {
            let tier = StorageClass::try_from(buf.get_u8())
                .map_err(|e| -> Error { box_err!("invalid tier: {}", e) })?;
            let transit_after = StdDuration::from_secs(buf.get_u64_le());
            rules.push(StorageClassTransitRule {
                tier,
                transit_after,
            });
        }

        Ok(Self {
            default_tier,
            transit_rules: rules,
        })
    }

    fn sort(&mut self) {
        self.transit_rules
            .sort_by(|a, b| a.transit_after.cmp(&b.transit_after));
    }

    pub fn target_storage_class(&self, elapsed: StdDuration) -> StorageClass {
        // `rev()`: Rules are sorted by transit_after in ascending order,
        self.transit_rules
            .iter()
            .rev()
            .find_map(|rule| (elapsed >= rule.transit_after).then_some(rule.tier))
            .unwrap_or(self.default_tier)
    }

    pub fn is_specified(&self) -> bool {
        self.default_tier.is_specified()
            || self
                .transit_rules
                .iter()
                .any(|rule| rule.tier.is_specified())
    }

    pub fn must_be_ia(&self) -> bool {
        self.default_tier.is_ia() && self.transit_rules.iter().all(|rule| rule.tier.is_ia())
    }

    // Note: Include the conditions that `must_be_ia()` is true.
    pub fn can_be_ia(&self) -> bool {
        self.default_tier.is_ia() || self.transit_rules.iter().any(|rule| rule.tier.is_ia())
    }

    pub fn is_auto_ia(&self) -> bool {
        self.can_be_ia() && !self.must_be_ia()
    }

    pub fn can_transit_to_other_sc(&self) -> bool {
        self.transit_rules
            .iter()
            .any(|rule| rule.tier != self.default_tier)
    }

    pub fn require_exclusive_region(&self) -> bool {
        self.default_tier.require_exclusive_region()
            || self
                .transit_rules
                .iter()
                .any(|rule| rule.tier.require_exclusive_region())
    }

    pub fn from_tidb(
        tier: Option<&str>,
        transitions: Option<&[StorageClassTransitionInfo]>,
    ) -> Self {
        let default_tier = StorageClass::from_tidb_storage_class_tier(tier);
        let transit_rules = transitions
            .map(|trans| {
                trans
                    .iter()
                    .map(|t| StorageClassTransitRule {
                        tier: StorageClass::from_tidb_storage_class_tier(Some(&t.tier)),
                        transit_after: StdDuration::from_secs(
                            t.after_days as u64 * 24 * 3600 + t.after_seconds as u64,
                        ),
                    })
                    .collect()
            })
            .unwrap_or_default();
        let mut spec = Self {
            default_tier,
            transit_rules,
        };
        spec.sort();
        spec
    }

    #[cfg(feature = "testexport")]
    pub fn to_tidb(&self) -> (Option<String>, Option<Vec<StorageClassTransitionInfo>>) {
        let tier = self
            .default_tier
            .is_specified()
            .then(|| self.default_tier.to_tidb());
        let transitions = (!self.transit_rules.is_empty()).then(|| {
            self.transit_rules
                .iter()
                .map(|rule| StorageClassTransitionInfo {
                    tier: rule.tier.to_tidb(),
                    after_days: (rule.transit_after.as_secs() / 86400) as u32,
                    after_seconds: (rule.transit_after.as_secs() % 86400) as u32,
                })
                .collect()
        });
        (tier, transitions)
    }

    pub fn from_schema_pb(schema: &kvenginepb::Schema) -> Self {
        let keys = schema.get_keys();
        let vals = schema.get_values();
        for i in 0..keys.len() {
            let key = &keys[i];
            let val = &vals[i];
            if key == STORAGE_CLASS_KEY {
                return Self::unmarshal(Some(val.as_slice()));
            }
        }
        Self::default()
    }

    pub fn apply_to_schema_pb(&self, schema: &mut kvenginepb::Schema) {
        schema.mut_keys().push(STORAGE_CLASS_KEY.to_string());
        schema.mut_values().push(self.marshal());
    }

    pub fn from_partition_pb(partition: &kvenginepb::Partition) -> Self {
        let keys = partition.get_keys();
        let vals = partition.get_values();
        for i in 0..keys.len() {
            let key = &keys[i];
            let val = &vals[i];
            if key == STORAGE_CLASS_KEY {
                return Self::unmarshal(Some(val.as_slice()));
            }
        }
        Self::default()
    }

    pub fn apply_to_partition_pb(&self, partition: &mut kvenginepb::Partition) {
        partition.mut_keys().push(STORAGE_CLASS_KEY.to_string());
        partition.mut_values().push(self.marshal());
    }
}

impl From<StorageClass> for StorageClassSpec {
    fn from(tier: StorageClass) -> Self {
        Self {
            default_tier: tier,
            transit_rules: vec![],
        }
    }
}

pub fn convert_column_infos_to_tipb(
    column_infos: &[ColumnInfo],
    pk_is_handle: bool,
) -> Result<Vec<tipb::ColumnInfo>> {
    let mut tipb_column_infos = Vec::with_capacity(column_infos.len());
    let mut ctx = EvalContext::default();
    for column_info in column_infos {
        let mut ci = tipb::ColumnInfo::new();
        ci.set_column_id(column_info.id);
        ci.set_tp(column_info.field_type.tp);
        ci.set_flag(column_info.field_type.flag);
        ci.set_column_len(column_info.field_type.flen.min(i32::MAX as i64) as i32);
        ci.set_decimal(column_info.field_type.decimal);
        ci.set_elems(
            column_info
                .field_type
                .elems
                .as_ref()
                .cloned()
                .unwrap_or_default()
                .into(),
        );
        ci.set_array(column_info.field_type.array.unwrap_or_default());
        let collation = box_try!(Collation::from_name(
            column_info.field_type.collate.as_str()
        ));
        ci.set_collation(collation as i32);
        let is_pk_handle = column_info.field_type.flag as u32 & FieldTypeFlag::PRIMARY_KEY.bits()
            != 0
            && pk_is_handle;
        ci.set_pk_handle(is_pk_handle);
        let default_val = box_try!(
            box_try!(decode_default_value_to_datum(&mut ctx, column_info))
                .map(|v| datum::encode_value(&mut EvalContext::default(), &[v]))
                .transpose()
        )
        .unwrap_or_default();
        ci.set_default_val(default_val);
        tipb_column_infos.push(ci);
    }
    Ok(tipb_column_infos)
}

// Ref: SetPBColumnsDefaultValue in TiDB.
// https://github.com/pingcap/tidb/blob/45318da24d8e4c0c6aab836d291a33f949dd18bf/pkg/table/tables/tables.go#L2303-L2329
// If origin_default is none, return None.
fn decode_default_value_to_datum(ctx: &mut EvalContext, c: &ColumnInfo) -> Result<Option<Datum>> {
    if !c.generated_expr_string.is_empty() && !c.generated_stored {
        return Ok(Some(Datum::Null));
    }

    // return None if default is none.
    let Some(default) = c.default.as_ref() else {
        return Ok(None);
    };

    let field_type_tp = box_try!(FieldTypeTp::from_i32(c.field_type.tp).ok_or_else(|| {
        tidb_query_datatype::codec::Error::InvalidDataType(format!(
            "Invalid field type: {}",
            c.field_type.tp
        ))
    }));
    let field_flag = c.field_type.flag;

    let result = match field_type_tp {
        FieldTypeTp::Tiny
        | FieldTypeTp::Short
        | FieldTypeTp::Int24
        | FieldTypeTp::Long
        | FieldTypeTp::LongLong => {
            if field_flag as u32 & FieldTypeFlag::UNSIGNED.bits() == FieldTypeFlag::UNSIGNED.bits()
            {
                default.parse::<u64>().map(|v| Datum::U64(v)).map_err(|e| {
                    tidb_query_datatype::codec::Error::InvalidDataType(format!(
                        "Invalid unsigned integer: {:?}",
                        e
                    ))
                })
            } else {
                default.parse::<i64>().map(|v| Datum::I64(v)).map_err(|e| {
                    tidb_query_datatype::codec::Error::InvalidDataType(format!(
                        "Invalid signed integer: {:?}",
                        e
                    ))
                })
            }
        }
        FieldTypeTp::Year => default.parse::<u64>().map(|v| Datum::U64(v)).map_err(|e| {
            tidb_query_datatype::codec::Error::InvalidDataType(format!(
                "Invalid signed integer: {:?}",
                e
            ))
        }),

        FieldTypeTp::Float | FieldTypeTp::Double => {
            default.parse::<f64>().map(|v| Datum::F64(v)).map_err(|e| {
                tidb_query_datatype::codec::Error::InvalidDataType(format!(
                    "Invalid float number: {:?}",
                    e
                ))
            })
        }
        FieldTypeTp::Date
        | FieldTypeTp::DateTime
        | FieldTypeTp::Timestamp
        | FieldTypeTp::NewDate => {
            // TODO: handle timezone
            let x = default.to_lowercase();
            if x == "current_timestamp" || x == "current_data" {
                Time::parse_datetime(
                    ctx,
                    chrono::Utc::now()
                        .format("%Y-%m-%d %H:%M:%S")
                        .to_string()
                        .as_str(),
                    c.field_type.decimal as i8,
                    false,
                )
                .map(|t| Datum::Time(t))
                .map_err(|e| {
                    tidb_query_datatype::codec::Error::InvalidDataType(format!(
                        "Invalid datetime: {:?}",
                        e
                    ))
                })
            } else if x == "0000-00-00 00:00:00" {
                Time::parse_from_i64(
                    ctx,
                    0,
                    box_try!(TimeType::try_from(field_type_tp)),
                    c.field_type.decimal as i8,
                )
                .map(|t| Datum::Time(t))
                .map_err(|e| {
                    tidb_query_datatype::codec::Error::InvalidDataType(format!(
                        "Invalid datetime: {:?}",
                        e
                    ))
                })
            } else {
                Time::parse(
                    ctx,
                    default.as_str(),
                    box_try!(TimeType::try_from(field_type_tp)),
                    c.field_type.decimal as i8,
                    false,
                )
                .map(|t| Datum::Time(t))
                .map_err(|e| {
                    tidb_query_datatype::codec::Error::InvalidDataType(format!(
                        "Invalid datetime: {:?}",
                        e
                    ))
                })
            }
        }

        FieldTypeTp::NewDecimal => Decimal::from_bytes(default.as_bytes())
            .map(|v| Datum::Dec(v.unwrap()))
            .map_err(|_| {
                tidb_query_datatype::codec::Error::InvalidDataType("Invalid decimal".to_string())
            }),
        FieldTypeTp::String | FieldTypeTp::VarString | FieldTypeTp::VarChar => {
            Ok(Datum::Bytes(default.as_bytes().to_vec()))
        }
        FieldTypeTp::Duration => {
            parse_duration(default.as_str(), c.field_type.decimal as i8).map(|v| Datum::Dur(v))
        }
        FieldTypeTp::Enum => {
            let elems = c.field_type.elems.as_ref().ok_or_else(|| {
                tidb_query_datatype::codec::Error::InvalidDataType(format!(
                    "Invalid enum value: {} (no enum elements defined)",
                    default
                ))
            })?;
            let value = elems
                .iter()
                .position(|e| e == default.as_str())
                .map(|pos| pos + 1)
                .ok_or_else(|| {
                    tidb_query_datatype::codec::Error::InvalidDataType(format!(
                        "Invalid enum value: {} (not found in enum elements: {:?})",
                        default, elems
                    ))
                })?;

            Ok(Datum::Enum(Enum::new(
                default.as_bytes().to_vec(),
                value as u64,
            )))
        }
        FieldTypeTp::Set => {
            let default_items = default.split(',').map(|s| s.trim()).collect::<Vec<&str>>();
            let mut buffer_vec = BufferVec::new();
            for item in &default_items {
                buffer_vec.push(item);
            }
            let elems = c.field_type.elems.as_ref().ok_or_else(|| {
                tidb_query_datatype::codec::Error::InvalidDataType(format!(
                    "Invalid set value: {} (no set elements defined)",
                    default
                ))
            })?;
            let mut value_bitmap: u64 = 0;
            for item in &default_items {
                let offset = elems.iter().position(|e| e == item).ok_or_else(|| {
                    tidb_query_datatype::codec::Error::InvalidDataType(format!(
                        "Invalid set value: '{}' (not found in set elements: {:?})",
                        item, elems
                    ))
                })?;
                value_bitmap |= 1 << offset;
            }
            Ok(Datum::Set(Set::new(Arc::new(buffer_vec), value_bitmap)))
        }
        FieldTypeTp::Null => Ok(Datum::Null),
        FieldTypeTp::Bit => {
            let Some(value_bit) = c.default_bit.as_ref() else {
                return Ok(None);
            };
            let value = base64::decode(value_bit).map_err(|e| {
                tidb_query_datatype::codec::Error::InvalidDataType(format!(
                    "Invalid bit value: {:?}",
                    e
                ))
            })?;
            assert!(value.len() <= 8);
            // Padding the value to 8 bytes with prefix 0.
            let mut padded_value = vec![0u8; 8];
            padded_value[8 - value.len()..].copy_from_slice(&value);
            Ok(Datum::U64(u64::from_be_bytes(
                padded_value.try_into().unwrap(),
            )))
        }
        FieldTypeTp::TiDbVectorFloat32 => {
            let vec_f32: Vec<f32> = serde_json::from_str(default).map_err(|_| {
                tidb_query_datatype::codec::Error::InvalidDataType(format!(
                    "Invalid vector float32 value: {}",
                    default
                ))
            })?;
            Ok(Datum::VectorFloat32(VectorFloat32::copy_from_f32(&vec_f32)))
        }
        FieldTypeTp::Unspecified
        | FieldTypeTp::TinyBlob
        | FieldTypeTp::MediumBlob
        | FieldTypeTp::LongBlob
        | FieldTypeTp::Blob
        | FieldTypeTp::Json => Ok(Datum::Bytes(vec![])),
        FieldTypeTp::Geometry => Err(tidb_query_datatype::codec::Error::InvalidDataType(
            "unsupported field type geometry".to_string(),
        )),
    };
    if let Ok(datum) = result {
        Ok(Some(datum))
    } else {
        Ok(Some(Datum::Bytes(vec![])))
    }
}

fn parse_duration(input: &str, fsp: i8) -> tidb_query_datatype::codec::Result<Duration> {
    let parts: Vec<&str> = input.split(':').collect();
    if parts.len() != 3 {
        return Err(tidb_query_datatype::codec::Error::InvalidDataType(
            "Invalid input: must be in the format HH:MM:SS".to_string(),
        ));
    }

    let hours = parts[0].parse::<i64>().map_err(|_| {
        tidb_query_datatype::codec::Error::InvalidDataType("Invalid hours".to_string())
    })?;
    let minutes = parts[1].parse::<u64>().map_err(|_| {
        tidb_query_datatype::codec::Error::InvalidDataType("Invalid minutes".to_string())
    })?;
    let seconds = parts[2].parse::<u64>().map_err(|_| {
        tidb_query_datatype::codec::Error::InvalidDataType("Invalid seconds".to_string())
    })?;

    if minutes >= 60 || seconds >= 60 {
        return Err(tidb_query_datatype::codec::Error::InvalidDataType(
            "Minutes and seconds must be less than 60".to_string(),
        ));
    }

    let secs = if hours < 0 {
        -hours * 3600 - minutes as i64 * 60 - seconds as i64
    } else {
        hours * 3600 + minutes as i64 * 60 + seconds as i64
    };

    Duration::from_secs(secs, fsp)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tiflash_replica() {
        let str =
            r#"{"Count":1,"LocationLabels":["host"],"Available":true,"AvailablePartitionIDs":[1]}"#;
        let replica: TiFlashReplica = serde_json::from_str(str).unwrap();
        assert_eq!(replica.count, 1);
        assert_eq!(replica.location_labels, Some(vec!["host".to_string()]));
        assert_eq!(replica.available, true);
        assert_eq!(replica.available_partition_ids, Some(vec![1]));

        let str =
            r#"{"Count":2,"LocationLabels":null,"Available":true,"AvailablePartitionIDs":null}"#;
        let replica: TiFlashReplica = serde_json::from_str(str).unwrap();
        assert_eq!(replica.count, 2);
        assert_eq!(replica.location_labels, None);
        assert_eq!(replica.available, true);
        assert_eq!(replica.available_partition_ids, None);
    }

    #[test]
    fn test_storage_class() {
        let cases = vec![
            (StorageClass::Unspecified, None, vec![]),
            (StorageClass::Unspecified, Some(vec![]), vec![]),
            (StorageClass::Standard, Some(vec![1]), vec![1]),
            (StorageClass::Ia, Some(vec![2]), vec![2]),
        ];
        for (sc, input, output) in cases {
            assert_eq!(StorageClass::unmarshal(input.as_deref()), sc);
            assert_eq!(sc.marshal(), output);

            // Compatibility tests.
            let spec = StorageClassSpec::unmarshal(input.as_deref());
            assert_eq!(spec, StorageClassSpec::from(sc));
            assert_eq!(StorageClass::unmarshal(Some(&spec.marshal())), sc);
        }
    }

    #[test]
    fn test_storage_class_spec() {
        let cases = vec![
            StorageClassSpec::default(),
            StorageClassSpec::from(StorageClass::Ia),
            StorageClassSpec {
                default_tier: StorageClass::Standard,
                transit_rules: vec![StorageClassTransitRule {
                    tier: StorageClass::Ia,
                    transit_after: StdDuration::from_secs(3600),
                }],
            },
            StorageClassSpec {
                default_tier: StorageClass::Unspecified,
                transit_rules: vec![
                    StorageClassTransitRule {
                        tier: StorageClass::Standard,
                        transit_after: StdDuration::from_secs(3600),
                    },
                    StorageClassTransitRule {
                        tier: StorageClass::Ia,
                        transit_after: StdDuration::from_secs(7200),
                    },
                ],
            },
        ];
        for spec in cases {
            assert_eq!(StorageClassSpec::unmarshal(Some(&spec.marshal())), spec);
        }

        assert_eq!(
            StorageClassSpec::unmarshal(None),
            StorageClassSpec::default()
        )
    }
}

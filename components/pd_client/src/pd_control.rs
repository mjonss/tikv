// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, fmt, sync::Arc, time::Duration};

use bstr::ByteSlice;
use bytes::Bytes;
use http::{Method, StatusCode};
use kvproto::metapb;
use regex::Regex;
use security::{HttpClientError, RestfulClient, SecurityManager};
use serde::{Deserialize, Deserializer};
use slog_global::debug;
use tikv_util::{
    config::{ReadableDuration, ReadableSize},
    retry::sleep_async,
    time::Instant,
};

use crate::Config;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Http(HttpClientError),
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("PD server error: [{module}]{msg}")]
    PdError { module: String, msg: String },
    #[error("PD server error: {0}")]
    PdOtherError(String),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("Timeout error: {0}")]
    Timeout(String),
    #[error("Other error: {0}")]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

impl From<HttpClientError> for Error {
    fn from(err: HttpClientError) -> Self {
        match err {
            HttpClientError::Http(status, msg) => {
                let msg = msg.trim_matches([' ', '\n', '\r', '"']);
                if status == StatusCode::NOT_FOUND {
                    Self::NotFound(msg.to_string())
                } else if let Some(pd_err) = extract_pd_server_err(msg) {
                    pd_err
                } else {
                    Self::PdOtherError(msg.to_string())
                }
            }
            err => Self::Http(err),
        }
    }
}

fn extract_pd_server_err(msg: &str) -> Option<Error> {
    // e.g. [PD:operator:ErrAddOperator]failed to add operator, maybe already have
    // one
    lazy_static::lazy_static! {
        static ref RE: Regex =
            Regex::new(r"^\[(?P<module>.+)\](?P<msg>.+)$").unwrap();
    }
    RE.captures(msg).map(|caps| Error::PdError {
        module: caps["module"].to_string(),
        msg: caps["msg"].to_string(),
    })
}

pub type Result<T> = std::result::Result<T, Error>;

const PD_CONFIG_PATH: &str = "pd/api/v1/config";
const PD_REGIONS_STORE_PATH: &str = "pd/api/v1/regions/store";
const PD_KEYSPACE_PATH: &str = "pd/api/v2/keyspaces";
const PD_PLACEMENT_RULE_GROUP_PATH: &str = "pd/api/v1/config/placement-rule";
const PD_PLACEMENT_RULE_PATH: &str = "pd/api/v1/config/rule";
const PD_REGION_PATH: &str = "pd/api/v1/region";
const PD_STATS_REGION: &str = "pd/api/v1/stats/region";
const PD_HEALTH_PATH: &str = "health";
const PD_SCHEDULERS_PATH: &str = "pd/api/v1/schedulers";
const PD_OPERATORS_PATH: &str = "pd/api/v1/operators";
const PD_STORES_PATH: &str = "pd/api/v1/stores";
const PD_STORE_PATH: &str = "pd/api/v1/store";

const EVICT_LEADER_SCHEDULER: &str = "evict-leader-scheduler";

const TIFLASH_GROUP: &str = "tiflash";

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
pub struct RuleGroup {
    pub group_id: String,
    pub group_index: i64,
    pub group_override: bool,
    pub rules: Option<Vec<Rule>>,
}

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
pub struct Rule {
    pub group_id: String,
    pub id: String,
    pub index: i64,
    #[serde(rename = "override")]
    pub is_override: bool,
    pub start_key: String,
    pub end_key: String,
    pub role: String,
    pub is_witness: bool,
    pub count: u32,
}

/// PdControl provides access to HTTP APIs of PD, which are not included in gRPC
/// interface. It's also expected to act like the tool `pd-ctl`.
#[derive(Clone)]
pub struct PdControl {
    client: RestfulClient,
}

impl PdControl {
    pub fn new(config: Config, security_mgr: Arc<SecurityManager>) -> Result<Self> {
        let mut client = RestfulClient::new("pd_control", config.endpoints, security_mgr)?;
        // Some server errors are not retryable. Judge by PdControl itself.
        client.set_retry_on_server_error(false);
        Ok(Self { client })
    }

    pub fn set_timeout(&mut self, timeout: Duration) {
        self.client.set_timeout(timeout / 2, timeout);
    }

    pub async fn get_config(&self) -> Result<PdConfigFromApi> {
        self.client.get(PD_CONFIG_PATH).await.map_err(Into::into)
    }

    pub async fn set_config(&self, configs: &[(&str, &str)]) -> Result<()> {
        let mut params = serde_json::Map::new();
        for &(item, value) in configs {
            let json_value: serde_json::Value = serde_json::from_str(value)?;
            params.insert(item.to_string(), json_value);
        }
        let _: String = self.client.post(PD_CONFIG_PATH, &params).await?;
        Ok(())
    }

    pub async fn get_store_regions(&self, store_id: u64) -> Result<RegionsInfo> {
        let query = format!("{}/{}", PD_REGIONS_STORE_PATH, store_id);
        self.client.get(query).await.map_err(Into::into)
    }

    pub async fn get_keyspace_by_name(&self, keyspace_name: &str) -> Result<KeyspaceMeta> {
        fail::fail_point!("pd_ctl::mock_get_keyspace_by_name", |_| {
            let id: u32 = keyspace_name.strip_prefix("ks").unwrap().parse().unwrap();
            Ok(KeyspaceMeta {
                id,
                name: keyspace_name.to_string(),
                ..Default::default()
            })
        });

        let query = format!("{PD_KEYSPACE_PATH}/{}", keyspace_name);
        self.client.get(query).await.map_err(Into::into)
    }

    pub async fn create_keyspace(&self, params: CreateKeyspaceParams) -> Result<KeyspaceMeta> {
        self.client
            .post(PD_KEYSPACE_PATH, &params)
            .await
            .map_err(Into::into)
    }

    pub async fn get_tiflash_placement_rule_group(&self) -> Result<Option<RuleGroup>> {
        fail::fail_point!("pd_ctl::mock_no_tiflash_placement_rule_group", |_| Ok(None));

        let path = format!("{PD_PLACEMENT_RULE_GROUP_PATH}/{TIFLASH_GROUP}");
        self.client.get(path).await.map_err(Into::into)
    }

    pub async fn remove_tiflash_placement_rule_by_id(&self, rule_id: &str) -> Result<()> {
        let path = format!("{PD_PLACEMENT_RULE_PATH}/{TIFLASH_GROUP}/{rule_id}");
        let _ = self.client.delete(path).await?;
        Ok(())
    }

    // Ref: https://github.com/tidbcloud/pd-cse/blob/release-8.1-keyspace/server/api/region.go
    pub async fn get_region(&self, encoded_key: &[u8]) -> Result<Option<RegionInfo>> {
        let hex_key = log_wrappers::hex_encode_upper(encoded_key);
        let path = format!("{PD_REGION_PATH}/key/{}?format=hex", hex_key);
        let region_info: RegionInfo = self.client.get(path).await?;
        if region_info.id == 0 {
            Ok(None)
        } else {
            Ok(Some(region_info))
        }
    }

    // Ref: https://github.com/tidbcloud/pd-cse/blob/release-7.1-keyspace/server/api/stats.go
    pub async fn get_regions_number(&self) -> Result<i32> {
        let path = format!("{PD_STATS_REGION}?start_key=&end_key=&count=true");
        let region_stats: RegionStats = self.client.get(path).await?;
        Ok(region_stats.count)
    }

    // Ref: https://github.com/tidbcloud/pd-cse/blob/release-7.1-keyspace/server/api/health.go
    pub async fn health(&self) -> Result<bool> {
        let health: Health = self.client.get(PD_HEALTH_PATH).await?;
        Ok(health.health == "true")
    }

    // Ref: https://github.com/tidbcloud/pd-cse/blob/release-7.1-keyspace/server/api/scheduler.go
    // `scheduler_name` can be "all" to pause or resume all schedulers.
    // Pass `dur` as `Duration::ZERO` to resume the scheduler.
    pub async fn pause_or_resume_scheduler(
        &self,
        scheduler_name: &str,
        dur: Duration,
    ) -> Result<()> {
        let path = format!("{PD_SCHEDULERS_PATH}/{scheduler_name}");
        let params = SchedulerDelay {
            delay: dur.as_secs() as i64,
        };
        let body_data = Bytes::from(serde_json::to_vec(&params)?);
        let _ = self
            .client
            .request(path, Method::POST, Some(body_data))
            .await?;
        Ok(())
    }

    // Note: only when `status` is `Some(SchedulerStatus::Paused)` will return
    // schedulers with timestamps.
    pub async fn list_schedulers(&self, status: Option<SchedulerStatus>) -> Result<Vec<Scheduler>> {
        let status_str = match status {
            Some(SchedulerStatus::Paused) => "paused",
            Some(SchedulerStatus::Disabled) => "disabled",
            None => "",
        };
        let query = format!("{PD_SCHEDULERS_PATH}?status={status_str}&timestamp=1");
        match status {
            Some(SchedulerStatus::Paused) => self.client.get(query).await.map_err(Into::into),
            Some(SchedulerStatus::Disabled) | None => {
                let scheduler_names: Vec<String> = self.client.get(query).await?;
                Ok(scheduler_names
                    .into_iter()
                    .map(|name| Scheduler {
                        name,
                        ..Default::default()
                    })
                    .collect())
            }
        }
    }

    pub async fn create_scheduler(&self, param: CreateSchedulerParam) -> Result<()> {
        let body_data = Bytes::from(serde_json::to_vec(&param)?);
        let resp = self
            .client
            .request(PD_SCHEDULERS_PATH, Method::POST, Some(body_data))
            .await?;
        debug!("pd_control::create_scheduler"; "resp" => resp.to_str_lossy().as_ref());
        Ok(())
    }

    pub async fn remove_scheduler(&self, name: &str) -> Result<String> {
        let query = format!("{PD_SCHEDULERS_PATH}/{name}");
        let resp = self.client.delete(query).await?;
        Ok(resp.to_str_lossy().to_string())
    }

    // Ref: https://github.com/tidbcloud/pd-cse/blob/release-7.1-keyspace/server/api/operator.go
    pub async fn get_operators(&self) -> Result<Vec<Operator>> {
        let query = format!("{PD_OPERATORS_PATH}?object=1");
        self.client.get(query).await.map_err(Into::into)
    }

    pub async fn cancel_operator_by_region(&self, region_id: u64) -> Result<()> {
        let query = format!("{PD_OPERATORS_PATH}/{region_id}");
        let _ = self.client.delete(query).await?;
        Ok(())
    }

    pub async fn merge_regions(
        &self,
        source_region_id: u64,
        target_region_id: u64,
    ) -> Result<String> {
        let operator = RegionMergeOperator::new(source_region_id, target_region_id);
        let msg: Option<String> = self.client.post(PD_OPERATORS_PATH, &operator).await?;
        Ok(msg.unwrap_or_default())
    }

    pub async fn get_stores(&self) -> Result<Vec<StoreInfo>> {
        let resp: StoresInfoResponse = self.client.get(PD_STORES_PATH).await?;
        Ok(resp.stores)
    }

    pub async fn get_store(&self, store_id: u64) -> Result<StoreInfo> {
        self.client
            .get(format!("{PD_STORE_PATH}/{store_id}"))
            .await
            .map_err(Into::into)
    }

    pub async fn find_store_by_status_address(
        &self,
        status_address: &str,
    ) -> Result<Option<StoreInfo>> {
        let resp: StoresInfoResponse = self.client.get(PD_STORES_PATH).await?;
        Ok(resp
            .stores
            .into_iter()
            .find(|store| store.store.status_address == status_address))
    }

    /// Evict all leaders from the specified store.
    ///
    /// Return OK when all leaders are evicted or the timeout is reached.
    pub async fn evict_store_leaders(
        &self,
        store_id: u64,
        timeout: Duration,
    ) -> Result<(StoreInfo, String /* scheduler_name */)> {
        self.create_scheduler(CreateSchedulerParam {
            name: EVICT_LEADER_SCHEDULER.to_string(),
            store_id: Some(store_id),
        })
        .await?;

        let start_time = Instant::now_coarse();
        loop {
            let store = self.get_store(store_id).await?;
            if store.status.leader_count == 0 || start_time.saturating_elapsed() > timeout {
                return Ok((store, EVICT_LEADER_SCHEDULER.to_string()));
            }
            sleep_async(Duration::from_millis(500)).await;
        }
    }
}

#[cfg(feature = "testexport")]
impl PdControl {
    fn is_http_client_error_retryable(err: &HttpClientError) -> bool {
        matches!(err, HttpClientError::Timeout(_) | HttpClientError::Hyper(_))
    }

    pub async fn merge_regions_by_key(
        &self,
        encoded_source_key: &[u8],
        encoded_target_key: &[u8],
        timeout: Duration,
    ) -> Result<()> {
        use log_wrappers::Value as LogValue;
        use tikv_util::warn;

        let is_error_retryable = |err: &Error| match err {
            Error::Http(e) => Self::is_http_client_error_retryable(e),
            Error::PdError { .. } => {
                // Retry all PD errors for now. Some of them may not be retryable.
                true
            }
            _ => false,
        };

        let start = Instant::now_coarse();
        while start.saturating_elapsed() < timeout {
            let Some(source_region) = self.get_region(encoded_source_key).await? else {
                warn!("source region not found"; "key" => LogValue::key(encoded_source_key));
                sleep_async(Duration::from_millis(200)).await;
                continue;
            };
            let Some(target_region) = self.get_region(encoded_target_key).await? else {
                warn!("target region not found"; "key" => LogValue::key(encoded_target_key));
                sleep_async(Duration::from_millis(200)).await;
                continue;
            };

            if source_region.id == target_region.id {
                return Ok(());
            }

            if let Err(err) = self.merge_regions(source_region.id, target_region.id).await {
                if is_error_retryable(&err) {
                    warn!("merge_regions_by_key: retry for error: {:?}", err);
                    sleep_async(Duration::from_millis(200)).await;
                    continue;
                }
                return Err(err);
            }
            sleep_async(Duration::from_millis(500)).await;
        }
        let msg = format!(
            "merge regions timeout: {} -> {}",
            LogValue::key(encoded_source_key),
            LogValue::key(encoded_target_key)
        );
        Err(Error::Timeout(msg))
    }
}

#[derive(Default, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(default)]
pub struct KeyspaceMeta {
    pub id: u32,
    pub name: String,
    pub state: String,
    pub created_at: u64,
    pub state_changed_at: u64,
    pub config: HashMap<String, String>,
}

#[derive(Default, Serialize, Deserialize)]
#[serde(default)]
pub struct RegionInfo {
    pub id: u64,
    #[serde(deserialize_with = "from_hex")]
    pub start_key: Vec<u8>,
    #[serde(deserialize_with = "from_hex")]
    pub end_key: Vec<u8>,
    pub approximate_size: u64,
}

impl fmt::Debug for RegionInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegionInfo")
            .field("id", &self.id)
            .field(
                "start_key",
                &log_wrappers::hex_encode_upper(&self.start_key),
            )
            .field("end_key", &log_wrappers::hex_encode_upper(&self.end_key))
            .field("approximate_size", &self.approximate_size)
            .finish()
    }
}

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
pub struct RegionsInfo {
    pub count: u64,
    pub regions: Vec<RegionInfo>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct PdScheduleConfig {
    pub max_store_down_time: String,
    pub max_merge_region_size: u64, // in MB.
    pub max_merge_region_keys: u64,
    pub split_merge_interval: ReadableDuration,
}

impl Default for PdScheduleConfig {
    fn default() -> Self {
        Self {
            max_store_down_time: "30m".to_string(),
            max_merge_region_size: 96,
            max_merge_region_keys: 200000,
            split_merge_interval: ReadableDuration::hours(1),
        }
    }
}

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct PdConfigFromApi {
    pub schedule: PdScheduleConfig,
}

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
pub struct RegionStats {
    pub count: i32,
}

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
pub struct Health {
    pub health: String, // Note: not a boolean.
}

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
pub struct CreateKeyspaceParams {
    pub name: String,
    pub config: HashMap<String, String>,
}

impl CreateKeyspaceParams {
    pub fn new(name: String) -> Self {
        Self {
            name,
            config: HashMap::new(),
        }
    }

    pub fn with_encryption(&mut self, enabled: bool) -> &mut Self {
        self.config.insert(
            KEYSPACE_CONFIG_ENCRYPTION_KEY.to_string(),
            serde_json::to_string(&KeyspaceConfigEncryption { enabled }).unwrap(),
        );
        self
    }
}

pub const KEYSPACE_CONFIG_ENCRYPTION_KEY: &str = "encryption";

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
pub struct KeyspaceConfigEncryption {
    pub enabled: bool,
}

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
pub struct SchedulerDelay {
    pub delay: i64,
}

pub enum SchedulerStatus {
    Paused,
    Disabled,
}

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
pub struct Scheduler {
    pub name: String,
    pub paused_at: String,
    pub resume_at: String,
}

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
pub struct CreateSchedulerParam {
    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub store_id: Option<u64>,
}

#[derive(Default, Deserialize, Debug)]
pub struct RegionEpoch {
    pub conf_ver: u64,
    pub version: u64,
}

impl From<&RegionEpoch> for metapb::RegionEpoch {
    fn from(epoch: &RegionEpoch) -> Self {
        let mut region_epoch = metapb::RegionEpoch::default();
        region_epoch.set_conf_ver(epoch.conf_ver);
        region_epoch.set_version(epoch.version);
        region_epoch
    }
}

pub type OpKindMask = u32; // bit mask of OpKind

// Ref: https://github.com/tidbcloud/pd-cse/blob/release-7.1-keyspace/pkg/schedule/operator/operator.go
#[derive(Default, Deserialize)]
pub struct Operator {
    pub desc: String,
    pub brief: String,
    pub region_id: u64,
    pub region_epoch: RegionEpoch,
    #[serde(rename = "kind")]
    pub kind_mask: OpKindMask,
}

impl std::fmt::Debug for Operator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Operator")
            .field("desc", &self.desc)
            .field("brief", &self.brief)
            .field("region_id", &self.region_id)
            .field("region_epoch", &self.region_epoch)
            .field("kind", &self.kind())
            .finish()
    }
}

impl Operator {
    pub fn kind(&self) -> Vec<OpKind> {
        let mut v = vec![];
        if self.kind_mask & OpKind::OpAdmin as u32 != 0 {
            v.push(OpKind::OpAdmin);
        }
        if self.kind_mask & OpKind::OpMerge as u32 != 0 {
            v.push(OpKind::OpMerge);
        }
        if self.kind_mask & OpKind::OpRange as u32 != 0 {
            v.push(OpKind::OpRange);
        }
        if self.kind_mask & OpKind::OpReplica as u32 != 0 {
            v.push(OpKind::OpReplica);
        }
        if self.kind_mask & OpKind::OpSplit as u32 != 0 {
            v.push(OpKind::OpSplit);
        }
        if self.kind_mask & OpKind::OpHotRegion as u32 != 0 {
            v.push(OpKind::OpHotRegion);
        }
        if self.kind_mask & OpKind::OpRegion as u32 != 0 {
            v.push(OpKind::OpRegion);
        }
        if self.kind_mask & OpKind::OpLeader as u32 != 0 {
            v.push(OpKind::OpLeader);
        }
        if self.kind_mask & OpKind::OpWitnessLeader as u32 != 0 {
            v.push(OpKind::OpWitnessLeader);
        }
        if self.kind_mask & OpKind::OpWitness as u32 != 0 {
            v.push(OpKind::OpWitness);
        }
        if v.is_empty() {
            v.push(OpKind::OpUnknown);
        }
        v
    }
}

// Ref: https://github.com/tidbcloud/pd-cse/blob/release-7.1-keyspace/pkg/schedule/operator/kind.go
#[derive(Debug, PartialEq)]
#[repr(u32)]
pub enum OpKind {
    OpUnknown = 0,
    OpAdmin = 1 << 0,
    // Initiated by merge checker or merge scheduler. Note that it may not include region merge.
    // the order describe the operator's producer and is very helpful to decouple scheduler or
    // checker limit
    OpMerge = 1 << 1,
    // Initiated by range scheduler.
    OpRange = 1 << 2,
    // Initiated by replica checker.
    OpReplica = 1 << 3,
    // Include region split. Initiated by rule checker if `kind & OpAdmin == 0`.
    OpSplit = 1 << 4,
    // Initiated by hot region scheduler.
    OpHotRegion = 1 << 5,
    // Include peer addition or removal or switch witness. This means that this operator may take
    // a long time.
    OpRegion = 1 << 6,
    // Include leader transfer.
    OpLeader = 1 << 7,
    // Include witness leader transfer.
    OpWitnessLeader = 1 << 8,
    // Include witness transfer.
    OpWitness = 1 << 9,
}

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
struct RegionMergeOperator {
    pub name: String,
    pub source_region_id: u64,
    pub target_region_id: u64,
}

impl RegionMergeOperator {
    pub fn new(source_region_id: u64, target_region_id: u64) -> Self {
        Self {
            name: "merge-region".to_string(),
            source_region_id,
            target_region_id,
        }
    }
}

// Ref: https://github.com/tidbcloud/pd-cse/blob/release-8.1-keyspace/pkg/response/store.go
#[derive(Default, Deserialize, Debug)]
pub struct StoresInfoResponse {
    pub count: u64,
    pub stores: Vec<StoreInfo>,
}

#[derive(Default, Deserialize, Debug)]
pub struct StoreInfo {
    pub store: MetaStore,
    pub status: StoreStatus,
}

#[derive(Default, Deserialize, Debug)]
pub struct MetaStore {
    pub id: u64,
    pub address: String,
    pub status_address: String,
    pub git_hash: String,
    pub state_name: String,
}

#[derive(Default, Deserialize, Debug)]
pub struct StoreStatus {
    pub capacity: ReadableSize,
    pub available: ReadableSize,
    pub leader_count: u64,
    pub region_count: u64,
}

fn from_hex<'de, D>(deserializer: D) -> std::result::Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    struct HexVisitor;

    impl serde::de::Visitor<'_> for HexVisitor {
        type Value = Vec<u8>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a hex string")
        }

        fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            hex::decode(value).map_err(serde::de::Error::custom)
        }
    }

    deserializer.deserialize_str(HexVisitor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_op_kind() {
        let mut op = Operator::default();
        assert_eq!(op.kind(), vec![OpKind::OpUnknown]);

        op.kind_mask = OpKind::OpMerge as u32;
        assert_eq!(op.kind(), vec![OpKind::OpMerge]);

        op.kind_mask = OpKind::OpSplit as u32;
        assert_eq!(op.kind(), vec![OpKind::OpSplit]);

        op.kind_mask = OpKind::OpRegion as u32;
        assert_eq!(op.kind(), vec![OpKind::OpRegion]);

        op.kind_mask = (OpKind::OpAdmin as u32) | (OpKind::OpReplica as u32);
        assert_eq!(op.kind(), vec![OpKind::OpAdmin, OpKind::OpReplica]);
    }

    #[test]
    fn test_extract_pd_server_err() {
        let msg = "[PD:operator:ErrAddOperator]failed to add operator, maybe already have one";
        let err = extract_pd_server_err(msg).unwrap();
        match err {
            Error::PdError { module, msg } => {
                assert_eq!(module, "PD:operator:ErrAddOperator");
                assert_eq!(msg, "failed to add operator, maybe already have one");
            }
            _ => panic!("expected PdError"),
        }

        let msg = "some other error message";
        assert!(extract_pd_server_err(msg).is_none());
    }
}

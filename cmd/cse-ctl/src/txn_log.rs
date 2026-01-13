// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{path::PathBuf, sync::Arc};

use clap::Args;
use kvengine::dfs::{DFSConfig, S3Fs};
use native_br::{
    common::create_pd_client,
    restore::{RestoreConfig, get_cluster_backup_meta},
    restore_keyspace::BackupCluster,
};
use raft_proto::eraftpb::{Entry, EntryType};
use rfstore::store::{PeerTag, RegionIdVer, rlog};
use security::SecurityConfig;
use txn_types::Lock;

#[derive(Args)]
pub struct ShowTxnLogArgs {
    /// The path of the config file.
    #[clap(long, default_value = "")]
    pub config: PathBuf,
    /// PD endpoints, use `,` to separate multiple PDs
    #[clap(long, default_value_t = String::new())]
    pub pd: String,
    /// The backup name to check table.
    #[clap(long)]
    pub backup_name: String,
    #[clap(long)]
    pub start_ts: Option<u64>,
    #[clap(long)]
    pub commit_ts: Option<u64>,
    #[clap(long)]
    pub data_dir: String,
    #[clap(long)]
    pub key_prefix: Option<String>,
    #[clap(long)]
    pub store_id: Option<u64>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Default)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ShowTxnLogConfig {
    pub pd: pd_client::Config,
    pub security: SecurityConfig,
    pub dfs: DFSConfig,
    pub data_dir: String,
}

impl ShowTxnLogConfig {
    fn from_args(args: &ShowTxnLogArgs) -> Self {
        let mut config = Self::default();
        if args.config.exists() {
            let data = std::fs::read(args.config.as_path()).expect("failed to read config file");
            config = toml::from_slice(&data).unwrap();
        }
        // override from args and ENV
        if !args.pd.is_empty() {
            config.pd.endpoints = args.pd.split(',').map(|x| x.to_owned()).collect();
        }
        config.dfs.override_from_env();
        config.security.override_from_env();
        if config.data_dir.is_empty() {
            config.data_dir = ".".to_string();
        }
        config
    }
}

pub(crate) fn execute_show_txn_log(args: ShowTxnLogArgs) {
    let config = ShowTxnLogConfig::from_args(&args);
    let pd_client = Arc::new(create_pd_client(&config.security, &config.pd));
    let s3fs = Arc::new(S3Fs::new_from_config(config.dfs));
    let mut cluster_backup = get_cluster_backup_meta(s3fs.as_ref(), args.backup_name.clone());
    let restore_conf = RestoreConfig {
        security: config.security.clone(),
        ..Default::default()
    };
    if let Some(store_id) = args.store_id {
        let mut stores = cluster_backup.take_stores().into_vec();
        stores.retain(|store| store.get_store_id() == store_id);
        cluster_backup.set_stores(stores.into());
    }
    let filter: Box<dyn LogFilter> = if let Some(key_prefix) = args.key_prefix {
        if let Ok(key_prefix) = hex::decode(key_prefix) {
            Box::new(KeyRangeFilter {
                key_prefix: key_prefix.to_vec(),
            })
        } else {
            panic!("key_prefix must be hex encoded");
        }
    } else if let Some(start_ts) = args.start_ts {
        Box::new(TxnFilter {
            start_ts,
            commit_ts: args.commit_ts.unwrap_or_default(),
        })
    } else {
        panic!("key_prefix or start_ts must be specified");
    };
    let cluster = BackupCluster::new(
        &cluster_backup,
        PathBuf::from(&config.data_dir),
        pd_client,
        s3fs.clone(),
        restore_conf,
        0,
        0,
        cluster_backup.backup_ts,
        true,
        None,
        false,
        None,
        None,
    )
    .unwrap();
    let mut txn_logs = vec![];
    for store_meta in cluster_backup.get_stores() {
        let store_id = store_meta.get_store_id();
        let rfengine = cluster.get_rfengine(store_id).unwrap();
        let region_peer_map = rfengine.get_region_peer_map();
        for (region_id, peer_id) in region_peer_map {
            let peer_tag = PeerTag::new(store_id, RegionIdVer::new(region_id, 0));
            let truncated_idx = rfengine.get_truncated_index(peer_id).unwrap();
            let last_index = rfengine.get_last_index(peer_id).unwrap_or_default();
            if last_index <= truncated_idx {
                continue;
            }
            let mut entries = vec![];
            rfengine
                .fetch_raft_entries_to(
                    peer_id,
                    truncated_idx + 1,
                    last_index + 1,
                    None,
                    &mut entries,
                )
                .unwrap();
            txn_logs.extend_from_slice(&parse_txn_log(peer_tag, filter.as_ref(), &entries));
        }
    }
    for log in txn_logs {
        println!("{}", log);
    }
}

trait LogFilter {
    fn filter(&self, key: &[u8], ts: u64) -> bool;
}

struct TxnFilter {
    start_ts: u64,
    commit_ts: u64,
}

impl LogFilter for TxnFilter {
    fn filter(&self, _key: &[u8], ts: u64) -> bool {
        ts == self.start_ts || ts == self.commit_ts
    }
}

struct KeyRangeFilter {
    key_prefix: Vec<u8>,
}

impl LogFilter for KeyRangeFilter {
    fn filter(&self, key: &[u8], _ts: u64) -> bool {
        key.starts_with(&self.key_prefix)
    }
}

fn parse_txn_log(tag: PeerTag, filter: &dyn LogFilter, entries: &[Entry]) -> Vec<String> {
    let mut txn_logs = vec![];
    for e in entries {
        if e.data.is_empty() || e.entry_type != EntryType::EntryNormal {
            continue;
        }
        let mut buf = vec![];
        let req = rfstore::store::parse_raft_cmd(tag, e, None, &mut buf);
        if let Some(cl) = rlog::get_custom_log(&req) {
            match cl.get_type() {
                rlog::CustomRaftLogType::Prewrite => cl.iterate_lock(|k, v| {
                    let mut lock = Lock::parse(v).unwrap();
                    lock.short_value.take();
                    if filter.filter(k, lock.ts.into_inner()) {
                        txn_logs.push(format!(
                            "{} prewrite log_idx:{} key:{} lock:{:?}",
                            tag,
                            e.index,
                            log_wrappers::hex_encode_upper(k),
                            lock
                        ));
                    }
                }),
                rlog::CustomRaftLogType::PessimisticLock => cl.iterate_lock(|k, v| {
                    let lock = Lock::parse(v).unwrap();
                    if filter.filter(k, lock.ts.into_inner()) {
                        txn_logs.push(format!(
                            "{} pessimistic_lock log_idx:{} key:{} lock:{:?}",
                            tag,
                            e.index,
                            log_wrappers::hex_encode_upper(k),
                            lock
                        ));
                    }
                }),
                rlog::CustomRaftLogType::Commit => cl.iterate_commit(|k, cm_ts| {
                    if filter.filter(k, cm_ts) {
                        txn_logs.push(format!(
                            "{} commit log_idx:{} key:{} ts:{}",
                            tag,
                            e.index,
                            log_wrappers::hex_encode_upper(k),
                            cm_ts
                        ));
                    }
                }),
                rlog::CustomRaftLogType::Rollback => cl.iterate_rollback(|k, st_ts, del_lock| {
                    if filter.filter(k, st_ts) {
                        txn_logs.push(format!(
                            "{} rollback log_idx:{} key:{} del_lock:{} ts:{}",
                            tag,
                            e.index,
                            log_wrappers::hex_encode_upper(k),
                            del_lock,
                            st_ts
                        ));
                    }
                }),
                rlog::CustomRaftLogType::ResolveLock => {
                    cl.iterate_resolve_lock(|tp, k, ts, del_lock| match tp {
                        rlog::CustomRaftLogType::Commit => {
                            if filter.filter(k, ts) {
                                txn_logs.push(format!(
                                    "{} resolve_lock commit log_idx:{} key:{} _del_lock:{}, ts:{}",
                                    tag,
                                    e.index,
                                    log_wrappers::hex_encode_upper(k),
                                    del_lock,
                                    ts
                                ));
                            }
                        }
                        rlog::CustomRaftLogType::Rollback => {
                            if filter.filter(k, ts) {
                                txn_logs.push(format!(
                                    "{} resolve_lock rollback log_idx:{} key:{}, ts:{}",
                                    tag,
                                    e.index,
                                    log_wrappers::hex_encode_upper(k),
                                    ts
                                ));
                            }
                        }
                        _ => {}
                    })
                }
                rlog::CustomRaftLogType::TxnFileRef => {
                    let txn_file_ref = cl.get_txn_file_ref().unwrap();
                    if filter.filter(&[], txn_file_ref.start_ts) {
                        txn_logs.push(format!(
                            "{} txn_file_ref log_idx:{} txn_file_ref:{:?}",
                            tag, e.index, txn_file_ref
                        ));
                    }
                }
                _ => {}
            }
        }
    }
    txn_logs
}

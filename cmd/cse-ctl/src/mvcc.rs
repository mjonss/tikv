use std::{path::PathBuf, sync::Arc};

use bytes::Buf;
use clap::Args;
use kvengine::{
    EXTRA_CF, LOCK_CF, UserMeta, WRITE_CF,
    dfs::{DFSConfig, new_dfs_from_config},
    encode_extra_txn_status_key,
};
use native_br::{
    common::create_pd_client,
    restore::{RestoreConfig, get_cluster_backup_meta},
    restore_keyspace::BackupCluster,
};
use security::SecurityConfig;

#[derive(Args)]
pub struct MvccArgs {
    /// The path of the config file.
    #[clap(long, default_value = "")]
    pub config: PathBuf,
    /// PD endpoints, use `,` to separate multiple PDs
    #[clap(long, default_value_t = String::new())]
    pub pd: String,
    /// The backup name to get mvcc info.
    #[clap(long)]
    pub backup_name: String,
    /// The keyspace id to get mvcc info.
    #[clap(long, default_value_t = 0)]
    pub keyspace_id: u32,
    /// hex encoded string, use ',' to separate multiple keys.
    #[clap(long, default_value_t = String::new())]
    pub keys: String,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Default)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct MvccConfig {
    pub pd: pd_client::Config,
    pub security: SecurityConfig,
    pub dfs: DFSConfig,
    pub data_dir: String,
    pub backup_name: String,
    pub keyspace_id: u32,
    pub keys: Vec<String>,
}

impl MvccConfig {
    fn from_args(args: &MvccArgs) -> Self {
        let mut config = Self::default();
        if args.config.exists() {
            let data = std::fs::read(args.config.as_path()).expect("failed to read config file");
            config = toml::from_slice(&data).unwrap();
        }
        // override from args and ENV
        if !args.pd.is_empty() {
            config.pd.endpoints = args.pd.split(',').map(|x| x.to_owned()).collect();
        }
        if !args.backup_name.is_empty() {
            config.backup_name = args.backup_name.clone();
        }
        if args.keyspace_id != 0 {
            config.keyspace_id = args.keyspace_id;
        }
        if !args.keys.is_empty() {
            config.keys = args.keys.split(',').map(|s| s.to_string()).collect();
        }
        config.dfs.override_from_env();
        config.security.override_from_env();
        if config.data_dir.is_empty() {
            config.data_dir = ".".to_string();
        }
        config
    }
}

pub fn execute_mvcc(args: MvccArgs) {
    let config = MvccConfig::from_args(&args);
    let pd_client = Arc::new(create_pd_client(&config.security, &config.pd));
    let dfs_cfg = config.dfs.clone();
    let dfs = new_dfs_from_config(dfs_cfg);
    let cluster_backup = get_cluster_backup_meta(dfs.as_ref(), args.backup_name.clone());
    let restore_conf = RestoreConfig {
        security: config.security.clone(),
        ..Default::default()
    };
    let cluster = BackupCluster::new(
        &cluster_backup,
        PathBuf::from(&config.data_dir),
        pd_client,
        dfs.clone(),
        restore_conf,
        config.keyspace_id,
        config.keyspace_id,
        cluster_backup.backup_ts,
        false,
        None,
        true,
        None,
        None,
    )
    .unwrap();
    let kv = cluster.get_kvengine();
    let all_id_vers = kv.get_all_shard_id_vers();
    let binary_keys = config
        .keys
        .iter()
        .map(|key| hex::decode(key).unwrap())
        .collect::<Vec<_>>();
    let mut shards = all_id_vers
        .iter()
        .filter_map(|id_ver| kv.get_shard(id_ver.id))
        .collect::<Vec<_>>();
    shards.sort_by(|a, b| a.range.outer_start.cmp(&b.range.outer_start));
    for key in &binary_keys {
        for shard in &shards {
            if key.as_slice() < shard.outer_start.chunk()
                || shard.outer_end.chunk() <= key.as_slice()
            {
                continue;
            }

            let snap = shard.new_snap_access();
            let mut iter = snap.new_iterator(WRITE_CF, false, true, None, false);
            iter.seek(key);
            while iter.valid() && iter.key() == key {
                let um = UserMeta::from_slice(iter.user_meta());
                let val = iter.val();
                println!(
                    "write key: {}, start_ts: {}, commit_ts: {}, value: {}",
                    hex::encode(key),
                    um.start_ts,
                    um.commit_ts,
                    hex::encode(val)
                );
                iter.next();
            }

            let lock_val = snap.get(LOCK_CF, key, 0);
            if !lock_val.is_value_empty() {
                let lock = txn_types::Lock::parse(lock_val.get_value()).unwrap();
                println!("lock key: {}, lock: {:?}", hex::encode(key), lock);
            }

            let extra_seek_key = encode_extra_txn_status_key(key, 0);
            let mut extra_iter = snap.new_iterator(EXTRA_CF, false, true, None, false);
            extra_iter.seek(&extra_seek_key);
            while extra_iter.valid()
                && extra_iter.key().starts_with(key)
                && extra_iter.key().len() == extra_seek_key.len()
            {
                let um = UserMeta::from_slice(iter.user_meta());
                println!(
                    "extra key: {}, start_ts: {}, commit_ts: {}",
                    hex::encode(key),
                    um.start_ts,
                    um.commit_ts,
                );
                extra_iter.next();
            }

            break;
        }
    }
}

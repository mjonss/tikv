use std::{
    fs,
    io::BufRead,
    mem,
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use api_version::ApiV2;
use clap::{Args, Subcommand};
use futures::executor::block_on;
use kvengine::table::OwnedInnerKey;
use native_br::common::create_pd_client;
use pd_client::{
    PdClient,
    pd_control::{PdControl, RegionInfo},
};
use security::{SecurityConfig, SecurityManager};
use tidb_query_datatype::codec::table::decode_table_id;
use tikv_util::config::{ReadableDuration, ReadableSize};
use txn_types::Key;

#[derive(Args)]
pub struct RegionCommand {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Merge regions based on keyspace and target size.
    Merge(RegionMergeArgs),
    /// Split region.
    Split(RegionSplitArgs),
    /// Batch split region.
    BatchSplit(RegionBatchSplitArgs),
}

pub fn execute_region_command(region_cmd: RegionCommand) {
    match region_cmd.command {
        Commands::Merge(args) => {
            execute_region_merge(args);
        }
        Commands::Split(args) => {
            execute_region_split(args);
        }
        Commands::BatchSplit(args) => {
            execute_region_batch_split(args);
        }
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Default)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct RegionCommandConfig {
    pub pd: pd_client::Config,
    pub security: SecurityConfig,
}

impl RegionCommandConfig {
    fn from_args(path: &Path, pd: &str) -> Self {
        let mut config = Self::default();
        if path.exists() {
            let data = std::fs::read(path).expect("failed to read config file");
            config = toml::from_slice(&data).expect("failed to parse config file");
        }
        // Override from args and ENV
        if !pd.is_empty() {
            config.pd.endpoints = pd.split(',').map(|x| x.to_owned()).collect();
        }
        config.security.override_from_env();
        config
    }
}

#[derive(Args)]
pub struct RegionMergeArgs {
    /// The path of the config file.
    #[clap(long, default_value = "")]
    pub config: PathBuf,
    /// PD endpoints, use `,` to separate multiple PDs
    #[clap(
        long,
        default_value = "http://serverless-cluster-pd.tidb-serverless.svc:2379"
    )]
    pub pd: String,
    /// The keyspace ID.
    #[clap(long)]
    keyspace_id: u32,
    /// The target region size.
    #[clap(long, default_value = "500MiB")]
    target_region_size: ReadableSize,
    /// The interval between each merge operation.
    #[clap(long, default_value = "5s")]
    interval: ReadableDuration,
    /// Can merge regions cross table.
    #[clap(long)]
    cross_table: bool,
    #[clap(long)]
    dry_run: bool,
    #[clap(long)]
    verbose: bool,
}

pub fn execute_region_merge(args: RegionMergeArgs) {
    let config = RegionCommandConfig::from_args(&args.config, &args.pd);
    let keyspace_id = args.keyspace_id;
    let target_region_size_mb = args.target_region_size.as_mb();

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _enter = runtime.enter();

    let ks_start_key = Key::from_raw(&ApiV2::get_keyspace_prefix_by_id(keyspace_id));
    let ks_end_key = Key::from_raw(&ApiV2::get_keyspace_prefix_by_id(keyspace_id + 1));
    if args.verbose {
        println!(
            "Start region merge, keyspace_id: {}, start_key: {:?}, end_key: {:?}",
            keyspace_id, ks_start_key, ks_end_key,
        );
    }

    let security_mgr =
        Arc::new(SecurityManager::new(&config.security).expect("create security manager"));
    let pd_ctl = PdControl::new(config.pd, security_mgr).expect("create PD control");

    let mut current_region = block_on(pd_ctl.get_region(ks_start_key.as_encoded()))
        .expect("get region")
        .expect("region is unavailable");

    while !current_region.end_key.is_empty() && &current_region.end_key < ks_end_key.as_encoded() {
        if args.verbose {
            println!("Current region: {:?}", current_region);
        }

        let next_region = block_on(pd_ctl.get_region(&current_region.end_key))
            .expect("get next region")
            .expect("next region is unavailable");

        if can_merge(&current_region, &next_region, target_region_size_mb, &args) {
            merge_region(&pd_ctl, &current_region, &next_region, &args);

            if next_region.end_key.is_empty() {
                break;
            }
            current_region = block_on(pd_ctl.get_region(&next_region.end_key))
                .expect("get next region")
                .expect("next region is unavailable");

            std::thread::sleep(args.interval.0);
        } else {
            current_region = next_region;
        }
    }
}

fn can_merge(
    source: &RegionInfo,
    target: &RegionInfo,
    target_region_size_mb: u64,
    args: &RegionMergeArgs,
) -> bool {
    // approximate_size == 1 is a special case, which means the region is empty, and
    // split by placement rule.
    source.approximate_size > 1
        && target.approximate_size > 1
        && source.approximate_size + target.approximate_size < target_region_size_mb * 3 / 2
        && (args.cross_table || from_same_table(source, target))
}

fn get_raw_inner_start_key(region: &RegionInfo) -> OwnedInnerKey {
    let raw_key = if region.start_key.is_empty() {
        vec![]
    } else {
        Key::from_encoded_slice(&region.start_key)
            .into_raw()
            .expect("decode start key")
    };
    OwnedInnerKey::new(raw_key.into())
}

fn get_raw_inner_end_key(region: &RegionInfo) -> OwnedInnerKey {
    let raw_key = if region.end_key.is_empty() {
        vec![]
    } else {
        Key::from_encoded_slice(&region.end_key)
            .into_raw()
            .expect("decode end key")
    };
    OwnedInnerKey::new_end_key(raw_key.into())
}

fn from_same_table(source: &RegionInfo, target: &RegionInfo) -> bool {
    let source_table_id =
        decode_table_id(get_raw_inner_start_key(source).as_ref().deref()).unwrap_or(0);
    let target_table_id =
        decode_table_id(get_raw_inner_end_key(target).as_ref().deref()).unwrap_or(i64::MAX);
    source_table_id == target_table_id
}

fn merge_region(
    pd_ctl: &PdControl,
    source: &RegionInfo,
    target: &RegionInfo,
    args: &RegionMergeArgs,
) {
    if !args.dry_run {
        match block_on(pd_ctl.merge_regions(source.id, target.id)) {
            Ok(msg) => {
                if args.verbose {
                    println!(
                        "Merge regions: {} -> {}: {} ({:?} -> {:?})",
                        source.id, target.id, msg, source, target
                    );
                } else {
                    println!("Merge regions: {} -> {}", source.id, target.id);
                }
            }
            Err(err) => {
                eprintln!(
                    "Warning: Failed to merge regions: {:?}, source {:?}, target {:?}",
                    err, source, target
                );
            }
        }
    } else {
        println!(
            "dry-run: Merge regions: {} -> {} ({:?} -> {:?})",
            source.id, target.id, source, target
        );
    }
}

#[derive(Args)]
pub struct RegionSplitArgs {
    /// The path of the config file.
    #[clap(long, default_value = "")]
    pub config: PathBuf,
    /// PD endpoints, use `,` to separate multiple PDs
    #[clap(
        long,
        default_value = "http://serverless-cluster-pd.tidb-serverless.svc:2379"
    )]
    pub pd: String,
    /// Raw split key in hex.
    #[clap(long)]
    pub raw_key: Option<String>,
    /// Encoded split key in hex.
    #[clap(long)]
    pub encoded_key: Option<String>,
}

pub fn execute_region_split(args: RegionSplitArgs) {
    let split_key = if let Some(raw_key) = args.raw_key {
        Key::from_raw(&hex::decode(raw_key).expect("hex decode"))
    } else if let Some(encoded_key) = args.encoded_key {
        Key::from_encoded(hex::decode(encoded_key).expect("hex decode"))
    } else {
        panic!("Either raw_key or encoded_key must be specified");
    };
    println!("Split region by key: {}", split_key);

    let config = RegionCommandConfig::from_args(&args.config, &args.pd);

    let pd_client = Arc::new(create_pd_client(&config.security, &config.pd));
    let new_regions_id = block_on(
        pd_client.split_regions_with_retry(vec![split_key.into_encoded()], Duration::from_secs(30)),
    )
    .expect("split region");
    println!("New regions: {:?}", new_regions_id);
}

#[derive(Args)]
pub struct RegionBatchSplitArgs {
    /// The path of the config file.
    #[clap(long, default_value = "")]
    pub config: PathBuf,
    /// PD endpoints, use `,` to separate multiple PDs
    #[clap(
        long,
        default_value = "http://serverless-cluster-pd.tidb-serverless.svc:2379"
    )]
    pub pd: String,
    /// Keys read from file, one key per line.
    #[clap(long)]
    pub keys_file: PathBuf,
    /// Keys are encoded or not.
    #[clap(long)]
    pub encoded: bool,
    /// Whether to scatter the new regions.
    #[clap(long)]
    pub scatter: bool,
    /// Batch size for splitting regions.
    #[clap(long, default_value_t = 64)]
    pub batch_size: usize,
    /// Split timeout.
    #[clap(long, default_value_t = ReadableDuration::secs(30))]
    pub timeout: ReadableDuration,
    /// Split retry limit.
    #[clap(long, default_value_t = 3)]
    pub retry: usize,
}

pub fn execute_region_batch_split(args: RegionBatchSplitArgs) {
    let config = RegionCommandConfig::from_args(&args.config, &args.pd);
    let pd_client = Arc::new(create_pd_client(&config.security, &config.pd));

    let file = fs::File::open(&args.keys_file).expect("failed to open keys file");
    let reader = std::io::BufReader::new(file);

    let mut keys = Vec::with_capacity(args.batch_size);

    fn split_region(pd: &dyn PdClient, keys: Vec<Vec<u8>>, args: &RegionBatchSplitArgs) {
        // Use `split_regions_opt` for best effort.
        // `split_regions_with_retry` will keep retrying even the key has been split.
        // TODO: PD should return the "have been split" keys as finished.
        let (new_regions, finished_percent) =
            match block_on(pd.split_regions_opt(keys, args.timeout.0, args.retry)) {
                Ok(res) => res,
                Err(e) => {
                    eprintln!("Failed to split regions: {}", e);
                    return;
                }
            };
        if args.scatter && !new_regions.is_empty() {
            if let Err(e) = pd.scatter_regions_by_id(new_regions.clone()) {
                eprintln!("Failed to split regions: {}", e);
                return;
            }
        }
        println!(
            "New regions: {:?}, finished percent: {}",
            new_regions, finished_percent
        );
    }

    for line in reader.lines() {
        let line = line.expect("failed to read line");
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let split_key = if args.encoded {
            Key::from_encoded(hex::decode(line).expect("hex decode"))
        } else {
            Key::from_raw(&hex::decode(line).expect("hex decode"))
        };
        keys.push(split_key.into_encoded());

        if keys.len() >= args.batch_size {
            split_region(&*pd_client, mem::take(&mut keys), &args);
        }
    }

    if !keys.is_empty() {
        split_region(&*pd_client, mem::take(&mut keys), &args);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_same_table() {
        let cases: Vec<(&'static str, &'static str, bool)> = vec![
            (
                "7800004274800000FF0000000A005F0000FE", // table id before "5F".
                "7800004274800000FF0000000A005F1000FE",
                true,
            ),
            (
                "7800004274800000FF0000000A005F0000FE",
                "7800004274800000FF0000000A015F0000FE",
                false,
            ),
            (
                "748000000000000AFF005F000000000000FA",
                "748000000000000AFF005F100000000000FA",
                true,
            ),
            (
                "748000000000000AFF005F000000000000FA",
                "748000000000000AFF015F000000000000FA",
                false,
            ),
            (
                "7800004200000000FB",
                "7800004274800000FF0000000A015F1000FE",
                false,
            ),
            ("7800004200000000FB", "7800004300000000FB", false),
            ("", "", false),
        ];

        for (source, target, expected) in cases {
            let source_region = RegionInfo {
                start_key: hex::decode(source).unwrap(),
                ..Default::default()
            };
            let target_region = RegionInfo {
                end_key: hex::decode(target).unwrap(),
                ..Default::default()
            };
            assert_eq!(
                from_same_table(&source_region, &target_region),
                expected,
                "{} {} {}",
                source,
                target,
                expected
            );
        }
    }
}

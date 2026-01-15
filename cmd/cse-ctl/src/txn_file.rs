// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{ops::Deref, path::PathBuf, sync::Arc};

use clap::Args;
use kvengine::{
    dfs,
    dfs::{DFSConfig, new_dfs_from_config},
    table::{TxnChunk, TxnChunkIterator, file::InMemFile, sstable::BlockCache},
};
use log_wrappers::Value;

#[derive(Args)]
pub struct ShowTxnChunkArgs {
    /// The path of the config file.
    #[clap(long, default_value = "")]
    pub config: PathBuf,
    /// The id of the txn chunk file.
    #[clap(long)]
    pub id: u64,
    /// The path of local txn chunk file.
    ///
    /// If specified, the txn chunk file will be get from the path instead of
    /// DFS.
    #[clap(long)]
    pub local: Option<PathBuf>,
    #[clap(long)]
    pub head: Option<usize>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Default)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ShowTxnChunkConfig {
    pub dfs: DFSConfig,
}

pub fn execute_show_txn_chunk(args: ShowTxnChunkArgs) {
    let mut config = ShowTxnChunkConfig::default();
    if args.config.exists() {
        let data = std::fs::read(args.config.clone()).expect("failed to read config file");
        config = toml::from_slice(&data).unwrap();
    }
    config.dfs.override_from_env();

    let data = match args.local {
        Some(local) => crate::sst::get_file_data_from_local(&local),
        None => get_txn_chunk_data_from_dfs(args.id, config),
    };
    let file = Arc::new(InMemFile::new(args.id, data));
    let txn_chunk =
        TxnChunk::new(file, BlockCache::None, None).expect("failed to create txn chunk");
    print_txn_chunk(&txn_chunk, 0);

    if let Some(mut head) = args.head {
        println!("Head {}:", head);
        let mut iter = TxnChunkIterator::new(txn_chunk, false);
        iter.rewind();
        while iter.valid() && head > 0 {
            println!(
                "{}: {}",
                Value::key(iter.key().deref()),
                Value::value(iter.get_value())
            );
            iter.next();
            head -= 1;
        }
    }
}

fn get_txn_chunk_data_from_dfs(id: u64, config: ShowTxnChunkConfig) -> bytes::Bytes {
    let dfs = new_dfs_from_config(config.dfs);
    let runtime = dfs.get_runtime();
    let opts = dfs::Options::default().with_type(dfs::FileType::TxnChunk);
    runtime
        .block_on(dfs.read_file(id, opts))
        .expect("failed to read file from dfs")
}

pub(crate) fn print_txn_chunk(txn_chunk: &TxnChunk, indent: usize) {
    let indent = " ".repeat(indent);
    println!("{}size: {}", indent, txn_chunk.size());
    println!("{}inserts: {}", indent, txn_chunk.get_inserts());
    println!(
        "{}check_non_exists: {}",
        indent,
        txn_chunk.get_check_non_exists()
    );
    println!(
        "{}check_constraint_blocks: {}",
        indent,
        txn_chunk.get_check_constraint_blocks()
    );
    let chunk_index = txn_chunk.get_index();
    println!("{}num_blocks: {}", indent, chunk_index.num_blocks());
    println!(
        "{}smallest: {}",
        indent,
        log_wrappers::hex_encode_upper(chunk_index.smallest().deref())
    );
    println!(
        "{}biggest: {}",
        indent,
        log_wrappers::hex_encode_upper(chunk_index.biggest().deref())
    );
}

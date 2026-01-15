// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use clap::Args;
use kvengine::{
    dfs::{DFSConfig, FileType, Options, new_dfs_from_config},
    table::{file::InMemFile, schema_file::SchemaFile},
};

#[derive(Args)]
pub struct ShowSchemaArgs {
    /// The path of the config file.
    #[clap(long, default_value = "")]
    pub config: PathBuf,
    /// The id of the Schema file.
    #[clap(long, default_value_t = 0)]
    pub id: u64,
    /// The path of local Schema file.
    ///
    /// If specified, the Schema file will be retrieved from the path instead of
    /// DFS.
    #[clap(long)]
    pub local: Option<PathBuf>,
    /// The table id to show.
    #[clap(long)]
    pub table_id: Option<i64>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Default)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ShowSchemaConfig {
    pub dfs: DFSConfig,
}

impl ShowSchemaConfig {
    pub fn from_env() -> Self {
        let mut config = Self::default();
        config.dfs.override_from_env();
        config
    }
}

pub(crate) fn get_file_data_from_local(local: &Path) -> bytes::Bytes {
    let data = std::fs::read(local).unwrap_or_else(|err| {
        panic!("failed to read local file from {:?}: {:?}", local, err);
    });
    bytes::Bytes::from(data)
}

fn get_file_data_from_dfs(id: u64, config: ShowSchemaConfig) -> bytes::Bytes {
    let dfs = new_dfs_from_config(config.dfs);
    let runtime = dfs.get_runtime();
    runtime
        .block_on(dfs.read_file(id, Options::default().with_type(FileType::Schema)))
        .expect("failed to read file from dfs")
}

fn parse_file_id(path: &Path) -> u64 {
    let file_prefix = path.file_prefix().unwrap().to_str().unwrap();
    u64::from_str_radix(file_prefix, 16).unwrap()
}

pub(crate) fn execute_show_schema(args: ShowSchemaArgs) {
    let config = ShowSchemaConfig::from_env();
    let (file_id, schema_file_data) = if let Some(local) = args.local {
        (parse_file_id(&local), get_file_data_from_local(&local))
    } else {
        (args.id, get_file_data_from_dfs(args.id, config))
    };
    let file = InMemFile::new(file_id, schema_file_data);
    let schema_file = SchemaFile::open(Arc::new(file)).unwrap();
    println!("[Schema file]");
    println!("  id: {}", schema_file.get_file_id());
    println!("  keyspace_id: {}", schema_file.get_keyspace_id());
    println!("  version: {}", schema_file.get_version());
    println!("  restore_version: {}", schema_file.get_restore_version());
    let tables = schema_file.export_schemas();
    if let Some(table_id) = args.table_id {
        println!("  table: {}", table_id);
        println!("    schema: {:?}", tables.get(&table_id).unwrap());
        return;
    }
    for (table_id, schema) in tables {
        println!("    table: {}", table_id);
        println!("    schema: {:?}", schema);
    }
}

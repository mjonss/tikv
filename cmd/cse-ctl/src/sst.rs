// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    ffi::OsStr,
    fs,
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
};

use clap::Args;
use futures::future::join_all;
use kvengine::{
    IoContext,
    dfs::{DFSConfig, FileType, Options, new_dfs_from_config},
    table::{
        blobtable::{blobtable::BlobTable, builder::BlobFooter},
        file::{File, InMemFile},
        sstable::{BlockCache, Footer, L0Footer, L0Table, SsTable},
    },
};

#[derive(Args)]
pub struct ShowSstArgs {
    /// The path of the config file.
    #[clap(long, default_value = "")]
    pub config: PathBuf,
    /// The id of the SST file.
    #[clap(long, default_value_t = 0)]
    pub id: u64,
    /// The path of local SST file.
    ///
    /// If specified, the SST file will be retrieved from the path instead of
    /// DFS.
    #[clap(long)]
    pub local: Option<PathBuf>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Default)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ShowSstConfig {
    pub dfs: DFSConfig,
}

pub(crate) fn get_file_data_from_local(local: &Path) -> bytes::Bytes {
    let data = std::fs::read(local).unwrap_or_else(|err| {
        panic!("failed to read local file from {:?}: {:?}", local, err);
    });
    bytes::Bytes::from(data)
}

fn get_file_data_from_dfs(id: u64, config: ShowSstConfig) -> bytes::Bytes {
    let dfs = new_dfs_from_config(config.dfs);
    let runtime = dfs.get_runtime();
    runtime
        .block_on(dfs.read_file(id, Options::default().with_type(FileType::Sst)))
        .expect("failed to read file from dfs")
}

#[derive(Debug)]
enum SstType {
    Unknown,
    L0,
    L1Plus,
    Blob,
}

fn detect_sst_type(file: &dyn File) -> SstType {
    // L1+ tables should be checked before L0 tables.
    if file.size() >= SsTable::footer_size() as u64 {
        let mut footer = Footer::default();
        footer.unmarshal(&file.read_footer(SsTable::footer_size()).unwrap());
        if footer.is_match() {
            return SstType::L1Plus;
        }
    }

    if file.size() >= L0Table::footer_size() as u64 {
        let mut footer = L0Footer::default();
        footer.unmarshal(&file.read_footer(L0Table::footer_size()).unwrap());
        if footer.is_match() {
            return SstType::L0;
        }
    }

    if file.size() >= BlobTable::footer_size() as u64 {
        let mut footer = BlobFooter::default();
        footer.unmarshal(&file.read_footer(BlobTable::footer_size()).unwrap());
        if footer.is_match() {
            return SstType::Blob;
        }
    }
    SstType::Unknown
}

pub fn execute_show_sst(args: ShowSstArgs) {
    let mut config = ShowSstConfig::default();
    if args.config.exists() {
        let data = std::fs::read(args.config.clone()).expect("failed to read config file");
        config = toml::from_slice(&data).unwrap();
    }
    config.dfs.override_from_env();

    let data = match args.local {
        Some(local) => get_file_data_from_local(&local),
        None => get_file_data_from_dfs(args.id, config),
    };
    let file = Arc::new(InMemFile::new(args.id, data));
    let sst_type = detect_sst_type(file.as_ref());
    match sst_type {
        SstType::L0 => {
            let l0 = L0Table::new(file, BlockCache::None, false, None)
                .unwrap()
                .unwrap();
            print_l0_table(&l0);
        }
        SstType::Blob => {
            let blob = BlobTable::new(file).unwrap();
            print_blob_table(&blob);
        }
        SstType::L1Plus => {
            let ln = SsTable::new(file, BlockCache::None, None).unwrap();
            println!("[SST {}, level 1+]", ln.id());
            print_sstable(&ln, 2);
        }
        SstType::Unknown => {
            eprintln!("Not a valid SST file");
        }
    }
}

pub(crate) fn print_l0_table(l0: &L0Table) {
    println!("[SST {}, level 0]", l0.id());
    println!("  size: {}", l0.size());
    println!("  max_ts: {}", l0.max_ts());
    println!("  entries: {}", l0.entries());
    println!("  tombs: {}", l0.tombs());
    println!("  entries_write_cf: {}", l0.entries_write_cf());
    println!("  kv_size: {}", l0.kv_size());
    println!("  version: {}", l0.snap_version());
    println!(
        "  smallest: {}",
        log_wrappers::hex_encode_upper(l0.smallest().deref())
    );
    println!(
        "  biggest: {}",
        log_wrappers::hex_encode_upper(l0.biggest().deref())
    );
    println!("  total_blob_size: {}", l0.total_blob_size());

    for cf in 0..kvengine::NUM_CFS {
        println!("  [CF {}]", cf);
        if let Some(tbl) = l0.get_cf(cf) {
            print_sstable(tbl, 4);
        } else {
            println!("    None");
        }
    }
}

pub(crate) fn print_blob_table(blob: &BlobTable) {
    println!("[BLOB {}]", blob.id());
    println!("  version: {}", blob.version());
    println!("  size: {}", blob.size());
    println!(
        "  smallest: {}",
        log_wrappers::hex_encode_upper(blob.smallest_key().deref())
    );
    println!(
        "  biggest: {}",
        log_wrappers::hex_encode_upper(blob.biggest_key().deref())
    );
    println!("  total_blob_size: {}", blob.total_blob_size());
    println!("  compression_tp: {}", blob.compression_tp());
    println!("  min_blob_size: {}", blob.min_blob_size());
}

pub(crate) fn print_sstable(tbl: &SsTable, indent: usize) {
    let indent = " ".repeat(indent);
    println!("{}size: {}", indent, tbl.size());
    println!("{}index_size: {}", indent, tbl.index_size());
    println!("{}filter_size: {}", indent, tbl.filter_size());
    println!("{}max_ts: {}", indent, tbl.max_ts);
    println!("{}entries: {}", indent, tbl.entries);
    println!("{}old_entries: {}", indent, tbl.old_entries);
    println!("{}tombs: {}", indent, tbl.tombs);
    println!("{}kv_size: {}", indent, tbl.kv_size);
    println!(
        "{}smallest: {}",
        indent,
        log_wrappers::hex_encode_upper(tbl.smallest().deref())
    );
    println!(
        "{}biggest: {}",
        indent,
        log_wrappers::hex_encode_upper(tbl.biggest().deref())
    );
    println!("{}compression_type: {}", indent, tbl.compression_type());
    println!("{}total_blob_size: {}", indent, tbl.total_blob_size());
    println!("{}encryption_ver: {}", indent, tbl.encryption_ver());

    let idx = tbl.load_index();
    println!("{}num_blocks: {}", indent, idx.num_blocks());
}

#[derive(Args)]
pub struct ScanBadTableFileArgs {
    /// The local path to scan SST files.
    #[clap(long)]
    pub path: PathBuf,
    #[clap(long, default_value_t = 4)]
    pub concurrency: usize,
}

pub fn execute_scan_bad_table(args: ScanBadTableFileArgs) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .max_blocking_threads(args.concurrency + 1)
        .enable_all()
        .build()
        .unwrap();
    if args.path.is_dir() {
        let handles = iterate_dir(&args.path, &rt);
        rt.block_on(join_all(handles));
    } else {
        eprintln!("{} is not a directory", args.path.display());
    }
}

fn iterate_dir(path: &Path, rt: &tokio::runtime::Runtime) -> Vec<tokio::task::JoinHandle<()>> {
    let mut handles = vec![];
    let read_dir = fs::read_dir(path).ctx("read_dir").unwrap();
    for entry in read_dir.flatten() {
        let sub_path = entry.path();
        if sub_path.is_dir() {
            handles.extend(iterate_dir(&sub_path, rt));
        } else {
            let h = rt.spawn_blocking(move || handle_table_file(&sub_path));
            handles.push(h)
        }
    }
    handles
}

fn handle_table_file(path: &Path) {
    if path.extension().is_none_or(|ext| ext != OsStr::new("sst")) {
        // TODO: support other file types
        return;
    }
    let filename = path.file_name().unwrap().to_string_lossy();
    let data = get_file_data_from_local(path);
    let file = Arc::new(InMemFile::new(0, data));
    let sst_type = detect_sst_type(file.as_ref());
    match sst_type {
        SstType::L0 => {
            let _ = L0Table::new(file, BlockCache::None, false, None)
                .unwrap_or_else(|err| panic!("{}({:?}) is invalid: {:?}", filename, sst_type, err))
                .unwrap();
        }
        SstType::Blob => {
            let _ = BlobTable::new(file)
                .unwrap_or_else(|err| panic!("{}({:?}) is invalid {:?}", filename, sst_type, err));
        }
        SstType::L1Plus => {
            let _ = SsTable::new(file, BlockCache::None, None)
                .unwrap_or_else(|err| panic!("{}({:?}) is invalid: {:?}", filename, sst_type, err));
        }
        SstType::Unknown => {
            panic!("{} is not a SST file", filename);
        }
    }
    println!("{filename}({sst_type:?}) is OK");
}

// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
#![feature(path_file_prefix)]

#[macro_use]
extern crate serde_derive;

mod archive;
mod backup;
mod check_columnar;
mod check_table;
mod common;
mod dfsgc;
mod http;
mod mvcc;
mod recovery;
mod region;
mod resolve_lock;
mod restore;
mod schema_file;
mod sst;
mod stats;
mod test;
mod txn_file;
mod txn_log;
mod unsafe_recover;

use std::{env, fs::OpenOptions, io, sync::LazyLock};

use backup::{ShowBackupListArgs, execute_show_backup_list};
use clap::{Args, Parser, Subcommand};
use native_br::common::step_to_stdout;
use region::{RegionCommand, execute_region_command};
use slog::Drain;

use crate::{
    Commands::*,
    archive::{ArchiveArgs, ShowArchiveArgs, execute_archive, execute_show_archive},
    backup::{BackupArgs, ShowBackupArgs, execute_backup, execute_show_backup},
    check_columnar::{CheckColumnarArgs, execute_check_columnar},
    check_table::{CheckTableArgs, execute_check_table},
    dfsgc::{DfsGcArgs, execute_dfsgc},
    http::HttpArgs,
    mvcc::{MvccArgs, execute_mvcc},
    recovery::{RecoveryArgs, execute_recovery},
    resolve_lock::{ResolveLockArgs, execute_resolve_lock},
    restore::{RestoreCommand, execute_restore_command},
    schema_file::{ShowSchemaArgs, execute_show_schema},
    sst::{ScanBadTableFileArgs, ShowSstArgs, execute_scan_bad_table, execute_show_sst},
    stats::{StatsArgs, execute_stats},
    test::{TestArgs, execute_test},
    txn_file::{ShowTxnChunkArgs, execute_show_txn_chunk},
    txn_log::{ShowTxnLogArgs, execute_show_txn_log},
    unsafe_recover::{UnsafeRecoverArgs, execute_unsafe_recover},
};

fn main() {
    init_logger();
    step_to_stdout();
    let x: Cli = Cli::parse();
    match x.command {
        DfsGc(dfsgc_arg) => {
            execute_dfsgc(dfsgc_arg);
        }
        UnsafeRecover(unsafe_recover) => {
            execute_unsafe_recover(unsafe_recover);
        }
        Backup(backup_args) => {
            execute_backup(backup_args);
        }
        Restore(restore_cmd) => {
            execute_restore_command(restore_cmd);
        }
        Archive(archive_args) => {
            execute_archive(archive_args);
        }
        Stats(stats_arg) => {
            execute_stats(stats_arg);
        }
        ResolveLock(arg) => {
            execute_resolve_lock(arg);
        }
        CheckTable(args) => {
            execute_check_table(args);
        }
        Mvcc(args) => execute_mvcc(args),
        Show(args) => {
            execute_show(args);
        }
        Http(args) => {
            http::execute_http(args);
        }
        Recovery(args) => {
            execute_recovery(args);
        }
        Test(args) => {
            execute_test(args);
        }
        Region(region_cmd) => {
            execute_region_command(region_cmd);
        }
        CheckColumnar(args) => {
            execute_check_columnar(args);
        }
    }
}

fn init_logger() {
    let output = env::var("LOG_FILE").ok();
    let level = tikv_util::logger::get_level_by_string(
        &env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_owned()),
    )
    .unwrap();
    let append_instead_truncate = env::var("LOG_APPEND").is_ok();

    match output {
        Some(log_file) => {
            let f = OpenOptions::new()
                .create(true)
                .write(!append_instead_truncate)
                .truncate(!append_instead_truncate)
                .append(append_instead_truncate)
                .open(log_file)
                .unwrap();
            init_logger_impl(f, level);
        }
        None => init_logger_impl(io::stdout(), level),
    };
}

fn init_logger_impl<W: 'static + io::Write + Send>(writer: W, level: slog::Level) {
    let drain: Box<dyn slog::Drain<Ok = (), Err = std::io::Error> + Send> =
        if atty::is(atty::Stream::Stdout) {
            let dec = slog_term::TermDecorator::new().build();
            Box::new(slog_term::CompactFormat::new(dec).build())
        } else {
            let dec = slog_term::PlainDecorator::new(writer);
            Box::new(slog_term::CompactFormat::new(dec).build())
        };
    let drain = std::sync::Mutex::new(drain).filter_level(level).fuse();
    let logger = slog::Logger::root(drain, slog::o!());
    slog_global::set_global(logger);
}

static VERSION_INFO: LazyLock<String> = LazyLock::new(|| {
    let build_timestamp = option_env!("TIKV_BUILD_TIME");
    tikv::tikv_version_info(build_timestamp)
});

#[derive(Parser)]
#[clap(author, version = &**VERSION_INFO, about, long_about = None)]
pub struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Scan and mark the unused DFS files as deleted.
    DfsGc(DfsGcArgs),
    /// Unsafely recover the cluster by directly modifying the data on the raft
    /// engine.
    UnsafeRecover(UnsafeRecoverArgs),
    /// Backup backups the cluster.
    Backup(BackupArgs),
    /// Restore a backup.
    Restore(RestoreCommand),
    /// Archive old backups.
    Archive(ArchiveArgs),
    /// Stats s3 objects.
    Stats(StatsArgs),
    /// Resolve lock
    ResolveLock(ResolveLockArgs),
    /// CheckTable check data consistency on each table.
    CheckTable(CheckTableArgs),
    /// CheckColumnar check columnar data consistency.
    CheckColumnar(CheckColumnarArgs),
    /// Mvcc gets the mvcc information of given keys.
    Mvcc(MvccArgs),
    /// Show some information.
    Show(ShowArgs),
    /// Execute recovery mode commands.
    Recovery(RecoveryArgs),
    /// Run HTTP requests to stores.
    Http(HttpArgs),
    /// Run test tools.
    Test(TestArgs),
    /// Region related commands.
    Region(RegionCommand),
}

#[derive(Args)]
pub struct ShowArgs {
    #[clap(subcommand)]
    command: ShowCommands,
}

#[derive(Subcommand)]
enum ShowCommands {
    /// Show the backup meta data.
    Backup(ShowBackupArgs),
    BackupList(ShowBackupListArgs),
    Sst(ShowSstArgs),
    ScanBadTableFile(ScanBadTableFileArgs),
    Archive(ShowArchiveArgs),
    TxnChunk(ShowTxnChunkArgs),
    TxnLog(ShowTxnLogArgs),
    Schema(ShowSchemaArgs),
}

fn execute_show(args: ShowArgs) {
    match args.command {
        ShowCommands::Backup(args) => {
            execute_show_backup(args);
        }
        ShowCommands::BackupList(args) => {
            execute_show_backup_list(args);
        }
        ShowCommands::Sst(args) => {
            execute_show_sst(args);
        }
        ShowCommands::ScanBadTableFile(args) => {
            execute_scan_bad_table(args);
        }
        ShowCommands::Archive(args) => {
            execute_show_archive(args);
        }
        ShowCommands::TxnChunk(args) => {
            execute_show_txn_chunk(args);
        }
        ShowCommands::TxnLog(args) => execute_show_txn_log(args),
        ShowCommands::Schema(args) => {
            execute_show_schema(args);
        }
    }
}

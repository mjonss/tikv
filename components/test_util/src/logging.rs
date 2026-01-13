// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    env, fmt,
    fs::{File, OpenOptions},
    io,
    io::prelude::*,
    sync::{Mutex, Once},
};

use slog::{self, Drain, OwnedKVList, Record};
use tikv_util::logger;

struct Serializer<'a>(&'a mut dyn std::io::Write);

impl slog::Serializer for Serializer<'_> {
    fn emit_arguments(&mut self, key: slog::Key, val: &std::fmt::Arguments<'_>) -> slog::Result {
        write!(self.0, ", {}: {}", key, val)?;
        Ok(())
    }
}

/// A logger that add a test case tag before each line of log.
struct CaseTraceLogger {
    f: Option<Mutex<File>>,
    skip_tags: Vec<&'static str>,
}

// FIXME: Remove this type when slog::Never implements Display.
#[derive(Debug)]
enum Never {}

impl fmt::Display for Never {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl CaseTraceLogger {
    fn write_log(
        w: &mut dyn std::io::Write,
        record: &Record<'_>,
        values: &OwnedKVList,
        skip_tags: &[&str],
    ) -> Result<(), std::io::Error> {
        use slog::KV;
        if skip_tags.contains(&record.tag()) {
            return Ok(());
        }

        let tag = tikv_util::get_tag_from_thread_name().map_or_else(|| "".to_owned(), |s| s + " ");
        let t = time::now();
        let time_str = time::strftime("%Y/%m/%d %H:%M:%S.%f", &t).unwrap();
        write!(
            w,
            "{}{} {}:{}: [{}] {}",
            tag,
            &time_str[..time_str.len() - 6],
            record.file().rsplit('/').next().unwrap(),
            record.line(),
            record.level(),
            record.msg(),
        )?;
        {
            let mut s = Serializer(w);
            record.kv().serialize(record, &mut s)?;
            values.serialize(record, &mut s)?;
        }
        writeln!(w)?;
        w.flush()?;
        Ok(())
    }
}

impl Drain for CaseTraceLogger {
    type Ok = ();
    type Err = Never;
    fn log(&self, record: &Record<'_>, values: &OwnedKVList) -> Result<Self::Ok, Self::Err> {
        if let Some(ref out) = self.f {
            let mut w = out.lock().unwrap();
            let _ = Self::write_log(&mut *w, record, values, &self.skip_tags);
        } else {
            let mut w = io::stderr();
            let _ = Self::write_log(&mut w, record, values, &self.skip_tags);
        }
        Ok(())
    }
}

impl Drop for CaseTraceLogger {
    fn drop(&mut self) {
        if let Some(ref w) = self.f {
            w.lock().unwrap().flush().unwrap();
        }
    }
}

// A help function to initial logger.
pub fn init_log_for_test() {
    init_log_for_test_with_opt(false);
}

/// AsyncLoggerGuard is used to collect remaining logs in the async logger on
/// normal exit. As reference of the async logger is held by the static
/// `ASYNC_LOGGER_GUARD`, it will not be dropped. So we use this guard to drop
/// it, and flush the remaining logs.
#[derive(Default)]
pub struct AsyncLoggerGuard {}

impl Drop for AsyncLoggerGuard {
    fn drop(&mut self) {
        // There might be remaining logs in the async logger.
        // To collect remaining logs and also collect future logs, replace the old one
        // with a terminal logger.
        // When the old global async logger is replaced, the old async guard will be
        // taken and dropped. In the drop() the async guard, it waits for the
        // finish of the remaining logs in the async logger.
        if let Some(level) = ::log::max_level().to_level() {
            let drainer = logger::text_format(logger::term_writer(), true);
            let _ = logger::init_log(
                drainer,
                logger::convert_log_level_to_slog_level(level),
                false, // Use sync logger to avoid an unnecessary log thread.
                false, // It is initialized already.
                vec![],
                0,
            );
        }
    }
}

// A help function to initial logger with async drainer.
// This is used for random test avoid performance issue.
#[must_use]
pub fn init_log_for_test_async() -> Option<AsyncLoggerGuard> {
    init_log_for_test_with_opt(true)
}

fn init_log_for_test_with_opt(use_async: bool) -> Option<AsyncLoggerGuard> {
    let mut guard = None;
    static START: Once = Once::new();
    START.call_once(|| {
        let output = env::var("LOG_FILE").ok();
        let level = tikv_util::logger::get_level_by_string(
            &env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_owned()),
        )
        .unwrap();
        let append_instead_truncate = env::var("LOG_APPEND").is_ok();
        let writer = output.map(|f| {
            Mutex::new(
                OpenOptions::new()
                    .create(true)
                    .write(!append_instead_truncate)
                    .truncate(!append_instead_truncate)
                    .append(append_instead_truncate)
                    .open(f)
                    .unwrap(),
            )
        });
        // We don't mind set it multiple times.
        // We hardly ever read rocksdb log in tests.
        let drainer = CaseTraceLogger {
            f: writer,
            skip_tags: vec![
                "rocksdb_log",
                "raftdb_log",
                "rocksdb_log_header",
                "raftdb_log_header",
            ],
        };

        // Default disabled log targets for test.
        let disabled_targets = vec!["tokio_core".to_owned(), "tokio_reactor".to_owned()];

        // CaseTraceLogger relies on test's thread name, however slog_async has
        // its own thread, and the name is "".
        // TODO: Enable the slog_async when the [Custom test frameworks][1] is mature,
        //       and hook the slog_async logger to every test cases.
        //
        // [1]: https://github.com/rust-lang/rfcs/blob/master/text/2318-custom-test-frameworks.md
        tikv_util::logger::init_log(
            drainer,
            level,
            use_async,
            true, // init std log
            disabled_targets,
            100,
        )
        .unwrap();

        if use_async {
            guard = Some(AsyncLoggerGuard::default());
        }
    });

    guard
}

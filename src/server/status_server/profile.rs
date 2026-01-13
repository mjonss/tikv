// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use std::pin::Pin;

use futures::{
    Future, FutureExt,
    future::BoxFuture,
    task::{Context, Poll},
};
use lazy_static::lazy_static;
use pprof::protos::Message;
use regex::Regex;
use tokio::sync::{Mutex, MutexGuard};

lazy_static! {
    // If it's locked it means there are already a heap or CPU profiling.
    static ref PROFILE_MUTEX: Mutex<()> = Mutex::new(());

    // To normalize thread names.
    static ref THREAD_NAME_RE: Regex =
        Regex::new(r"^(?P<thread_name>[a-z-_ :]+?)(-?\d)*$").unwrap();
    static ref THREAD_NAME_REPLACE_SEPERATOR_RE: Regex = Regex::new(r"[_ ]").unwrap();
}

type OnEndFn<I, T> = Box<dyn FnOnce(I) -> Result<T, String> + Send + 'static>;

struct ProfileGuard<'a, I, T> {
    _guard: MutexGuard<'a, ()>,
    item: Option<I>,
    on_end: Option<OnEndFn<I, T>>,
    end: BoxFuture<'static, Result<(), String>>,
}

impl<I, T> Unpin for ProfileGuard<'_, I, T> {}

impl<'a, I, T> ProfileGuard<'a, I, T> {
    fn new<F1, F2>(
        on_start: F1,
        on_end: F2,
        end: BoxFuture<'static, Result<(), String>>,
    ) -> Result<ProfileGuard<'a, I, T>, String>
    where
        F1: FnOnce() -> Result<I, String>,
        F2: FnOnce(I) -> Result<T, String> + Send + 'static,
    {
        let _guard = match PROFILE_MUTEX.try_lock() {
            Ok(guard) => guard,
            _ => return Err("Already in Profiling".to_owned()),
        };
        let item = on_start()?;
        Ok(ProfileGuard {
            _guard,
            item: Some(item),
            on_end: Some(Box::new(on_end) as OnEndFn<I, T>),
            end,
        })
    }
}

impl<I, T> Future for ProfileGuard<'_, I, T> {
    type Output = Result<T, String>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.end.as_mut().poll(cx) {
            Poll::Ready(res) => {
                let item = self.item.take().unwrap();
                let on_end = self.on_end.take().unwrap();
                let r = match (res, on_end(item)) {
                    (Ok(_), r) => r,
                    (Err(errmsg), _) => Err(errmsg),
                };
                Poll::Ready(r)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Trigger one cpu profile.
pub async fn start_one_cpu_profile<F>(
    end: F,
    frequency: i32,
    protobuf: bool,
) -> Result<Vec<u8>, String>
where
    F: Future<Output = Result<(), String>> + Send + 'static,
{
    let on_start = || {
        let guard = pprof::ProfilerGuardBuilder::default()
            .frequency(frequency)
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()
            .map_err(|e| format!("pprof::ProfilerGuardBuilder::build fail: {}", e))?;
        Ok(guard)
    };

    let on_end = move |guard: pprof::ProfilerGuard<'static>| {
        let report = guard
            .report()
            .frames_post_processor(move |frames| {
                let name = extract_thread_name(&frames.thread_name);
                frames.thread_name = name;
            })
            .build()
            .map_err(|e| format!("create cpu profiling report fail: {}", e))?;
        let mut body = Vec::new();
        if protobuf {
            let profile = report
                .pprof()
                .map_err(|e| format!("generate pprof from report fail: {}", e))?;
            profile
                .write_to_vec(&mut body)
                .map_err(|e| format!("encode pprof into bytes fail: {}", e))?;
        } else {
            report
                .flamegraph(&mut body)
                .map_err(|e| format!("generate flamegraph from report fail: {}", e))?;
        }
        Ok(body)
    };

    ProfileGuard::new(on_start, on_end, end.boxed())?.await
}

fn extract_thread_name(thread_name: &str) -> String {
    THREAD_NAME_RE
        .captures(thread_name)
        .and_then(|cap| {
            cap.name("thread_name").map(|thread_name| {
                THREAD_NAME_REPLACE_SEPERATOR_RE
                    .replace_all(thread_name.as_str(), "-")
                    .into_owned()
            })
        })
        .unwrap_or_else(|| thread_name.to_owned())
}

// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};

use collections::{HashMap, HashSet};
use http::Request;
use hyper::Body;
use parking_lot::Mutex;
use pd_client::PdClient;
use security::HttpClient;
use tikv_util::{Either, box_err, box_try, box_try_join, error, time::Instant, warn};
use tokio::sync::RwLock;

use crate::{common::send_request_to_store_with_retry_opt, error::Result};

// NOTE: request this URL before TiKV supports will actually request
// "/kvengine/files".
const KV_FILES_WITH_TYPE_API: &str = "/kvengine/files_with_type";

/// Manage the existed files on TiKV servers.
///
/// Used to estimate restore throughput.
pub struct StoresFiles {
    cache_ttl: Duration,
    req_timeout: Duration,

    pd_client: Arc<dyn PdClient>,
    http_client: HttpClient,
    runtime: tokio::runtime::Handle,

    stores: Mutex<HashMap<u64 /* store_id */, Arc<StoreFiles>>>,
}

pub trait FileWithId: Send + Sync + Clone + 'static {
    fn file_id(&self) -> u64;
}

impl StoresFiles {
    pub fn new(
        cache_ttl: Duration,
        req_timeout: Duration,
        pd_client: Arc<dyn PdClient>,
        runtime: tokio::runtime::Handle,
    ) -> Result<Self> {
        let http_client = box_try!(
            pd_client
                .get_security_mgr()
                .http_client(hyper::Client::builder())
        );
        Ok(Self {
            cache_ttl,
            req_timeout,
            pd_client,
            http_client,
            runtime,
            stores: Default::default(),
        })
    }

    pub async fn get_sst_files_nonexisted_on_stores<F: FileWithId>(
        &self,
        sst_files: Vec<F>,
        stores: Vec<u64>,
    ) -> Result<Vec<(u64 /* store_id */, Vec<F>)>> {
        let mut join_set = tokio::task::JoinSet::new();
        for store_id in stores {
            let store_files = self
                .stores
                .lock()
                .entry(store_id)
                .or_insert_with(|| {
                    Arc::new(StoreFiles::new(
                        store_id,
                        self.cache_ttl,
                        self.req_timeout,
                        self.pd_client.clone(),
                        self.http_client.clone(),
                    ))
                })
                .clone();
            let sst_files = sst_files.clone();
            let task = async move {
                let res = store_files.get_nonexisted_sst_files(sst_files).await;
                (store_id, res)
            };
            join_set.spawn_on(task, &self.runtime);
        }

        let mut err_count = 0;
        let mut results = Vec::with_capacity(join_set.len());
        while let Some(res) = join_set.join_next().await {
            let (store_id, (nonexisted_files, res)) = box_try_join!(res);
            match res {
                Ok(()) => {
                    results.push((store_id, nonexisted_files));
                }
                Err(err) => {
                    error!("StoreFiles: get nonexisted sst files from store failed: {:?}", err; "store" => store_id);

                    // Tolerate no more than 1 store (peer) failure.
                    err_count += 1;
                    if err_count > 1 {
                        return Err(err);
                    } else {
                        results.push((store_id, nonexisted_files));
                    }
                }
            }
        }
        Ok(results)
    }
}

// The status server interface "/kvengine/files_with_type" return
// `HashMap<String /* file_suffix */, HashSet<u64 /* file_id */>>`.
type FilesWithTypeResponse = HashMap<String /* file_suffix */, HashSet<u64 /* file_id */>>;

struct StoreFiles {
    store_id: u64,
    cache_ttl: Duration,
    req_timeout: Duration,

    pd_client: Arc<dyn PdClient>,
    http_client: HttpClient,

    existed_sst_files: RwLock<HashSet<u64>>,
    update_time_secs: AtomicI64,
}

impl StoreFiles {
    fn new(
        store_id: u64,
        cache_ttl: Duration,
        req_timeout: Duration,
        pd_client: Arc<dyn PdClient>,
        http_client: HttpClient,
    ) -> Self {
        Self {
            store_id,
            cache_ttl,
            req_timeout,
            pd_client,
            http_client,
            existed_sst_files: RwLock::new(HashSet::default()),
            update_time_secs: AtomicI64::new(0),
        }
    }

    fn update_time(&self) -> Instant {
        Instant::from_timespec_second_coarse(self.update_time_secs.load(Ordering::Relaxed))
    }

    fn set_update_time(&self, instant: Instant) {
        self.update_time_secs
            .store(instant.second(), Ordering::Relaxed);
    }

    async fn refresh(&self) -> Result<()> {
        let update_time = self.update_time();
        let now = Instant::now_coarse();
        if now.saturating_duration_since(update_time) <= self.cache_ttl {
            return Ok(());
        }

        let mut existed_sst_files = self.existed_sst_files.write().await;

        let store = box_try!(self.pd_client.get_store_async(self.store_id).await);
        let sec_mgr = self.pd_client.get_security_mgr();
        let uri = box_try!(sec_mgr.build_uri(format!(
            "{}{}?filter=sst",
            store.status_address, KV_FILES_WITH_TYPE_API
        )));
        let req = || Request::get(uri.clone()).body(Body::empty()).unwrap();
        let resp = send_request_to_store_with_retry_opt(
            req,
            &store,
            Either::Right(&self.http_client),
            self.req_timeout,
        )
        .await;
        match resp {
            Ok(resp) => {
                match serde_json::from_slice::<FilesWithTypeResponse>(&resp) {
                    Ok(mut files_with_type_resp) => {
                        *existed_sst_files = files_with_type_resp.remove("sst").unwrap_or_default();
                    }
                    Err(err) => {
                        // It should be TiKV not supporting this API.
                        // Consider as no files existed on this store.
                        warn!(
                            "StoreFiles: parse files_with_type response from store {} failed: {}",
                            self.store_id, err
                        );
                        debug_assert!(false);
                        existed_sst_files.clear();
                    }
                }
            }
            Err(err) => return Err(box_err!(err)),
        };

        self.set_update_time(now);
        drop(existed_sst_files);

        Ok(())
    }

    // Consider as all files nonexisted on error.
    // When refresh failed, it's likely that the store is down, and peers will be
    // moved to other stores. In this condition, the files are considered as not
    // existed on the store, and we should slow down the restore speed.
    async fn get_nonexisted_sst_files<F: FileWithId>(
        &self,
        mut input: Vec<F>,
    ) -> (Vec<F>, Result<()>) {
        if let Err(err) = self.refresh().await {
            return (input, Err(err));
        }

        let existed_sst_files = self.existed_sst_files.read().await;
        input.retain(|f| !existed_sst_files.contains(&f.file_id()));
        (input, Ok(()))
    }
}

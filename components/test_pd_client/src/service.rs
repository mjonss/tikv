// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use futures::executor::block_on;
use kvproto::pdpb::*;
use pd_client::{PdClient, errors::Error as ClientError};
use test_pd::{PdMocker, Result};
use tikv_util::time::UnixSecs;

use crate::TestPdClient;

/// Wrap `TestPdClient` to a `PdMocker` service to provide gRPC interface.
pub struct Service {
    inner: Arc<TestPdClient>,
}

impl Service {
    pub fn new(inner: Arc<TestPdClient>) -> Self {
        Self { inner }
    }

    pub fn header(&self) -> ResponseHeader {
        let mut header = ResponseHeader::default();
        header.set_cluster_id(self.inner.get_cluster_id().unwrap());
        header
    }
}

fn set_error_header(header: &mut ResponseHeader, e: &ClientError, err_type: ErrorType) {
    let mut err = Error::default();
    err.set_type(err_type);
    err.set_message(format!("{:?}", e));
    header.set_error(err);
}

impl PdMocker for Service {
    fn get_cluster_id(&self) -> u64 {
        self.inner.get_cluster_id().unwrap()
    }

    fn tso(&self, req: &TsoRequest) -> Option<Result<TsoResponse>> {
        let mut resp = TsoResponse::default();
        resp.set_header(self.header());
        match block_on(self.inner.batch_get_tso(req.get_count())) {
            Ok(ts) => {
                resp.mut_timestamp().set_physical(ts.physical() as i64);
                resp.mut_timestamp().set_logical(ts.logical() as i64);
                resp.set_count(req.get_count());
            }
            Err(e) => {
                set_error_header(resp.mut_header(), &e, ErrorType::Unknown);
            }
        }
        Some(Ok(resp))
    }

    fn bootstrap(&self, req: &BootstrapRequest) -> Option<Result<BootstrapResponse>> {
        let mut resp = BootstrapResponse::default();
        resp.set_header(self.header());
        match self
            .inner
            .bootstrap_cluster(req.get_store().clone(), req.get_region().clone())
        {
            Ok(replication_status) => {
                if let Some(replication_status) = replication_status {
                    resp.set_replication_status(replication_status);
                }
            }
            Err(e) => {
                set_error_header(resp.mut_header(), &e, ErrorType::AlreadyBootstrapped);
            }
        }
        Some(Ok(resp))
    }

    fn is_bootstrapped(&self, _: &IsBootstrappedRequest) -> Option<Result<IsBootstrappedResponse>> {
        let mut resp = IsBootstrappedResponse::default();
        resp.set_header(self.header());
        match self.inner.is_cluster_bootstrapped() {
            Ok(bootstrapped) => {
                resp.set_bootstrapped(bootstrapped);
            }
            Err(e) => {
                set_error_header(resp.mut_header(), &e, ErrorType::Unknown);
            }
        }
        Some(Ok(resp))
    }

    fn alloc_id(&self, _: &AllocIdRequest) -> Option<Result<AllocIdResponse>> {
        let mut resp = AllocIdResponse::default();
        resp.set_header(self.header());
        match self.inner.alloc_id() {
            Ok(id) => {
                resp.set_id(id);
            }
            Err(e) => {
                set_error_header(resp.mut_header(), &e, ErrorType::Unknown);
            }
        };
        Some(Ok(resp))
    }

    fn get_store(&self, req: &GetStoreRequest) -> Option<Result<GetStoreResponse>> {
        let mut resp = GetStoreResponse::default();
        resp.set_header(self.header());

        let store_id = req.get_store_id();
        match self.inner.get_store(store_id) {
            Ok(store) => {
                resp.set_store(store);
                if let Some(stats) = self.inner.get_store_stats(store_id) {
                    resp.set_stats(stats);
                }
            }
            Err(e) => {
                set_error_header(resp.mut_header(), &e, ErrorType::Unknown);
            }
        }
        Some(Ok(resp))
    }

    fn put_store(&self, req: &PutStoreRequest) -> Option<Result<PutStoreResponse>> {
        let mut resp = PutStoreResponse::default();
        resp.set_header(self.header());
        match self.inner.put_store(req.get_store().clone()) {
            Ok(replication_status) => {
                if let Some(replication_status) = replication_status {
                    resp.set_replication_status(replication_status);
                }
            }
            Err(e) => {
                set_error_header(resp.mut_header(), &e, ErrorType::Unknown);
            }
        }
        Some(Ok(resp))
    }

    fn get_all_stores(&self, req: &GetAllStoresRequest) -> Option<Result<GetAllStoresResponse>> {
        let mut resp = GetAllStoresResponse::default();
        resp.set_header(self.header());
        match self
            .inner
            .get_all_stores(req.get_exclude_tombstone_stores())
        {
            Ok(stores) => {
                resp.set_stores(stores.into());
            }
            Err(e) => {
                set_error_header(resp.mut_header(), &e, ErrorType::Unknown);
            }
        }
        Some(Ok(resp))
    }

    fn store_heartbeat(
        &self,
        req: &StoreHeartbeatRequest,
    ) -> Option<Result<StoreHeartbeatResponse>> {
        let stats = req.get_stats().clone();
        let report = req
            .has_store_report()
            .then(|| req.get_store_report().clone());
        let sync_status = req
            .has_dr_autosync_status()
            .then(|| req.get_dr_autosync_status().clone());
        let resp = match block_on(self.inner.store_heartbeat(stats, report, sync_status)) {
            Ok(mut resp) => {
                resp.set_header(self.header());
                resp
            }
            Err(e) => {
                let mut resp = StoreHeartbeatResponse::default();
                resp.set_header(self.header());
                set_error_header(resp.mut_header(), &e, ErrorType::Unknown);
                resp
            }
        };
        Some(Ok(resp))
    }

    fn region_heartbeat(
        &self,
        req: &RegionHeartbeatRequest,
    ) -> Option<Result<RegionHeartbeatResponse>> {
        let mut resp = RegionHeartbeatResponse::default();
        resp.set_header(self.header());

        let region_stat = pd_client::RegionStat {
            down_peers: req.get_down_peers().to_vec(),
            pending_peers: req.get_pending_peers().to_vec(),
            written_bytes: req.get_bytes_written(),
            written_keys: req.get_keys_written(),
            read_bytes: req.get_bytes_read(),
            read_keys: req.get_keys_read(),
            query_stats: req.get_query_stats().clone(),
            approximate_size: req.get_approximate_size(),
            approximate_keys: req.get_approximate_keys(),
            last_report_ts: UnixSecs::from(req.get_interval().get_start_timestamp()),
            cpu_usage: req.get_cpu_usage(),
            ..Default::default()
        };
        let replication_status = req
            .has_replication_status()
            .then(|| req.get_replication_status().clone());
        match block_on(self.inner.region_heartbeat(
            req.get_term(),
            req.get_region().clone(),
            req.get_leader().clone(),
            region_stat,
            replication_status,
        )) {
            Ok(_) => {}
            Err(e) => {
                set_error_header(resp.mut_header(), &e, ErrorType::Unknown);
            }
        }
        Some(Ok(resp))
    }

    fn get_region(&self, req: &GetRegionRequest) -> Option<Result<GetRegionResponse>> {
        let mut resp = GetRegionResponse::default();
        resp.set_header(self.header());
        match self.inner.get_region_info(req.get_region_key()) {
            Ok(region_info) => {
                resp.set_region(region_info.region);
                if let Some(leader) = region_info.leader {
                    resp.set_leader(leader);
                }
            }
            Err(e) => {
                set_error_header(resp.mut_header(), &e, ErrorType::RegionNotFound);
            }
        }
        Some(Ok(resp))
    }

    fn get_region_by_id(&self, req: &GetRegionByIdRequest) -> Option<Result<GetRegionResponse>> {
        let mut resp = GetRegionResponse::default();
        resp.set_header(self.header());
        match block_on(self.inner.get_region_leader_by_id(req.get_region_id())) {
            Ok(Some((region, leader))) => {
                resp.set_region(region);
                if leader.get_store_id() != 0 {
                    resp.set_leader(leader);
                }
            }
            Ok(None) => {
                let e = box_err!("region not found, region_id: {}", req.get_region_id());
                set_error_header(resp.mut_header(), &e, ErrorType::RegionNotFound);
            }
            Err(e) => {
                set_error_header(resp.mut_header(), &e, ErrorType::Unknown);
            }
        }
        Some(Ok(resp))
    }

    fn ask_split(&self, req: &AskSplitRequest) -> Option<Result<AskSplitResponse>> {
        let resp = match block_on(self.inner.ask_split(req.get_region().clone())) {
            Ok(mut resp) => {
                resp.set_header(self.header());
                resp
            }
            Err(e) => {
                let mut resp = AskSplitResponse::default();
                resp.set_header(self.header());
                set_error_header(resp.mut_header(), &e, ErrorType::Unknown);
                resp
            }
        };
        Some(Ok(resp))
    }

    fn ask_batch_split(&self, req: &AskBatchSplitRequest) -> Option<Result<AskBatchSplitResponse>> {
        let resp = match block_on(
            self.inner
                .ask_batch_split(req.get_region().clone(), req.get_split_count() as usize),
        ) {
            Ok(mut resp) => {
                resp.set_header(self.header());
                resp
            }
            Err(e) => {
                let mut resp = AskBatchSplitResponse::default();
                resp.set_header(self.header());
                set_error_header(resp.mut_header(), &e, ErrorType::Unknown);
                resp
            }
        };
        Some(Ok(resp))
    }

    fn report_batch_split(
        &self,
        req: &ReportBatchSplitRequest,
    ) -> Option<Result<ReportBatchSplitResponse>> {
        let mut resp = ReportBatchSplitResponse::default();
        resp.set_header(self.header());
        match block_on(self.inner.report_batch_split(req.get_regions().to_vec())) {
            Ok(_) => {}
            Err(e) => {
                set_error_header(resp.mut_header(), &e, ErrorType::Unknown);
            }
        }
        Some(Ok(resp))
    }

    fn get_cluster_config(
        &self,
        _: &GetClusterConfigRequest,
    ) -> Option<Result<GetClusterConfigResponse>> {
        let mut resp = GetClusterConfigResponse::default();
        resp.set_header(self.header());
        match self.inner.get_cluster_config() {
            Ok(cluster) => {
                resp.set_cluster(cluster);
            }
            Err(e) => {
                set_error_header(resp.mut_header(), &e, ErrorType::Unknown);
            }
        }
        Some(Ok(resp))
    }

    fn scatter_region(&self, req: &ScatterRegionRequest) -> Option<Result<ScatterRegionResponse>> {
        let mut resp = ScatterRegionResponse::default();
        resp.set_header(self.header());
        match self
            .inner
            .scatter_regions_by_id(req.get_regions_id().to_vec())
        {
            Ok(_) => {}
            Err(e) => {
                set_error_header(resp.mut_header(), &e, ErrorType::Unknown);
            }
        }
        Some(Ok(resp))
    }

    fn get_gc_safe_point(
        &self,
        _: &GetGcSafePointRequest,
    ) -> Option<Result<GetGcSafePointResponse>> {
        let mut resp = GetGcSafePointResponse::default();
        resp.set_header(self.header());
        match block_on(self.inner.get_gc_safe_point()) {
            Ok(gc_safe_point) => {
                resp.set_safe_point(gc_safe_point);
            }
            Err(e) => {
                set_error_header(resp.mut_header(), &e, ErrorType::Unknown);
            }
        }
        Some(Ok(resp))
    }

    fn update_gc_safe_point(
        &self,
        req: &UpdateGcSafePointRequest,
    ) -> Option<Result<UpdateGcSafePointResponse>> {
        let mut resp = UpdateGcSafePointResponse::default();
        resp.set_header(self.header());
        match self.inner.set_gc_safe_point(req.get_safe_point()) {
            Ok(new_safe_point) => {
                resp.set_new_safe_point(new_safe_point);
            }
            Err(e) => {
                set_error_header(resp.mut_header(), &e, ErrorType::Unknown);
            }
        }
        Some(Ok(resp))
    }

    fn get_operator(&self, req: &GetOperatorRequest) -> Option<Result<GetOperatorResponse>> {
        let resp = match self.inner.get_operator(req.get_region_id()) {
            Ok(mut resp) => {
                resp.set_header(self.header());
                resp
            }
            Err(e) => {
                let mut resp = GetOperatorResponse::default();
                resp.set_header(self.header());
                set_error_header(resp.mut_header(), &e, ErrorType::Unknown);
                resp
            }
        };
        Some(Ok(resp))
    }
}

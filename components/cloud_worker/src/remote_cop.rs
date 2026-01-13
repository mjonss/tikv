// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    net::SocketAddr,
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::{FutureExt, TryFutureExt};
use grpcio::{ChannelBuilder, EnvBuilder, Environment, RpcStatus, RpcStatusCode, ServerBuilder};
use kvproto::{
    coprocessor::{DelegateRequest, Request, Response},
    tikvpb::{Tikv, TikvClient, create_tikv},
};
use tikv::coprocessor::parse_request_and_handle_remote_cop;
use tikv_util::{info, thd_name, time::Instant, warn};

use crate::server::{Context, get_cop_req_tag};

#[derive(Clone, Default, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub addr: String,
    pub max_handle_duration: Duration,
}

pub struct RemoteCopServer {
    grpc_server: grpcio::Server,
}

impl RemoteCopServer {
    pub(crate) fn new(ctx: Arc<Context>, cfg: Config) -> RemoteCopServer {
        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(16)
                .name_prefix(thd_name!("grpc-server"))
                .build(),
        );
        let channel_args = ChannelBuilder::new(env.clone())
            .stream_initial_window_size(2 * 1024 * 1024)
            .max_concurrent_stream(1024)
            .max_receive_message_len(-1)
            .max_send_message_len(-1)
            .http2_max_ping_strikes(i32::MAX) // For pings without data from clients.
            .keepalive_time(Duration::from_secs(10))
            .keepalive_timeout(Duration::from_secs(3))
            .build_args();
        let addr = SocketAddr::from_str(&cfg.addr).unwrap();
        let cop_service = CopService::new(ctx, env.clone(), cfg);
        let security_mgr = cop_service.ctx.pd.get_security_mgr();
        let sb = ServerBuilder::new(env)
            .channel_args(channel_args)
            .register_service(create_tikv(cop_service));
        let sb = security_mgr.bind(sb, &addr.ip().to_string(), addr.port());
        let grpc_server = sb.build().unwrap();
        Self { grpc_server }
    }

    pub fn start(&mut self) {
        self.grpc_server.start()
    }
}

#[derive(Clone)]
pub struct CopService {
    ctx: Arc<Context>,
    env: Arc<Environment>,
    store_addrs: Arc<Mutex<HashMap<u64, String>>>,
    channels: Arc<Mutex<HashMap<String, TikvClient>>>,
    cfg: Config,
}

impl CopService {
    fn new(ctx: Arc<Context>, env: Arc<Environment>, cfg: Config) -> Self {
        Self {
            ctx,
            env,
            store_addrs: Arc::new(Mutex::new(HashMap::new())),
            channels: Arc::new(Mutex::new(HashMap::new())),
            cfg,
        }
    }
}

impl Tikv for CopService {
    fn coprocessor(
        &mut self,
        ctx: grpcio::RpcContext<'_>,
        req: Request,
        sink: grpcio::UnarySink<Response>,
    ) {
        let handle_start = Instant::now_coarse();
        let store_id = req.get_context().get_peer().get_store_id();
        let client_res = self.get_client(store_id);
        if let Err(err) = client_res {
            warn!("get client failed"; "err" => ?err);
            ctx.spawn(
                sink.fail(RpcStatus::with_message(
                    RpcStatusCode::INTERNAL,
                    err.to_string(),
                ))
                .unwrap_or_else(|e| {
                    warn!("failed to send rpc status"; "err" => ?e);
                }),
            );
            return;
        }
        let tag = get_cop_req_tag(&req);
        let client = client_res.unwrap();
        let mut delegate_req = DelegateRequest::new();
        let key_ranges = req.get_ranges().to_vec();
        delegate_req.set_context(req.get_context().clone());
        delegate_req.set_ranges(key_ranges.into());
        delegate_req.set_start_ts(req.get_start_ts());
        let max_handle_duration = self.cfg.max_handle_duration;
        let snap_ctx = self.ctx.get_snap_ctx(None);
        let quota_limiter = self.ctx.quota_limiter.clone();
        let mem_limiter = self.ctx.memory_limiter.clone();
        let peer = Some(ctx.peer());
        let max_resp_size = self.ctx.config.cop_max_resp_size.0;
        let future = async move {
            let delegate_start = Instant::now_coarse();
            let mut resp = client
                .delegate_coprocessor_async(&delegate_req)
                .unwrap()
                .await
                .map_err(|e| tikv::coprocessor::Error::Other(format!("{:?}", e)))?;

            let snap_start = Instant::now_coarse();
            let (snap_access, mem_limiter_guard) = kvengine::SnapAccess::construct_snapshot(
                &tag,
                &snap_ctx,
                &resp.take_mem_table_data(),
                &resp.take_snapshot(),
                mem_limiter,
            )
            .await
            .map_err(|e| tikv::coprocessor::Error::Other(format!("{:?}", e)))?;
            let snapshot = rfstore::store::RegionSnapshot::from_snapshot(snap_access, None);

            let process_start = Instant::now_coarse();
            let result = parse_request_and_handle_remote_cop(
                req,
                peer,
                max_handle_duration,
                max_resp_size,
                quota_limiter.clone(),
                snapshot,
            )
            .await;
            drop(mem_limiter_guard);

            if let Ok(response) = &result {
                let finish_time = Instant::now_coarse();
                let wait_dur = delegate_start.saturating_duration_since(handle_start);
                let delegate_req_dur = snap_start.saturating_duration_since(delegate_start);
                let snap_dur = process_start.saturating_duration_since(snap_start);
                let process_dur = finish_time.saturating_duration_since(process_start);
                let handle_dur = finish_time.saturating_duration_since(handle_start);
                info!(
                    "finished remote cop service";
                    "tag" => tag,
                    "resp_size" => response.data.len(),
                    "max_handle_duration" => ?max_handle_duration,
                    "wait_dur" => ?wait_dur,
                    "delegate_req_dur" => ?delegate_req_dur,
                    "snap_dur" => ?snap_dur,
                    "process_dur" => ?process_dur,
                    "handle_dur" => ?handle_dur,
                );
            }
            result
        };
        let task = async move {
            match future.await {
                Ok(mut resp) => sink.success(resp.consume()).await?,
                Err(err) => {
                    sink.fail(RpcStatus::with_message(
                        RpcStatusCode::INTERNAL,
                        err.to_string(),
                    ))
                    .await?
                }
            }
            Ok(())
        }
        .map_err(|e: grpcio::Error| {
            warn!("remote cop failed";
                "request" => "coprocessor",
                "err" => ?e
            );
        })
        .map(|_| ());
        ctx.spawn(task);
    }
}

impl CopService {
    fn resolve_store_addr(&mut self, store_id: u64) -> pd_client::Result<String> {
        let mut addrs = self.store_addrs.lock().unwrap();
        if let Some(addr) = addrs.get(&store_id) {
            return Ok(addr.clone());
        }
        let store = self.ctx.pd.get_store(store_id)?;
        addrs.insert(store_id, store.address.clone());
        Ok(store.address)
    }

    fn get_client(&mut self, store_id: u64) -> pd_client::Result<TikvClient> {
        let addr = self.resolve_store_addr(store_id)?;
        let mut channels = self.channels.lock().unwrap();
        if let Some(channel) = channels.get(&addr) {
            return Ok(channel.clone());
        }
        let cb = ChannelBuilder::new(self.env.clone());
        let security_mgr = self.ctx.pd.get_security_mgr();
        let channel = security_mgr.connect(cb, &addr);
        let client = TikvClient::new(channel);
        channels.insert(addr, client.clone());
        Ok(client)
    }
}

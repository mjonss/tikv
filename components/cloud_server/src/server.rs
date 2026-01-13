// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    error::Error as StdError,
    ffi::CString,
    io::Error as IoError,
    net::{AddrParseError, IpAddr, SocketAddr},
    result,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use api_version::KvFormat;
use engine_traits::Error as EngineTraitError;
use futures::{channel::oneshot::Canceled, compat::Stream01CompatExt, stream::StreamExt};
use grpcio::{
    ChannelBuilder, Environment, Error as GrpcError, ResourceQuota, Server as GrpcServer,
    ServerBuilder,
};
use grpcio_health::{HealthService, ServingStatus, create_health};
use hyper::Error as HttpError;
use kvproto::tikvpb::*;
use openssl::error::ErrorStack as OpenSslError;
use pd_client::Error as PdError;
use protobuf::ProtobufError;
use rfstore::{Error as RaftServerError, router::RaftStoreRouter};
use security::SecurityManager;
use thiserror::Error;
use tikv::{
    coprocessor::Endpoint,
    read_pool::ReadPool,
    server::{
        Config, Proxy,
        load_statistics::*,
        metrics::{MEMORY_USAGE_GAUGE, SERVER_INFO_GAUGE_VEC},
        resolve::StoreAddrResolver,
    },
    storage::{
        Error as StorageError, Storage, kv::Error as EngineError, lock_manager::LockManager,
    },
    tikv_build_version,
};
use tikv_util::{
    Either,
    codec::Error as CodecError,
    config::VersionTrack,
    sys::{get_global_memory_usage, record_global_memory_usage},
    timer::GLOBAL_TIMER_HANDLE,
};
use tokio::runtime::{Builder as RuntimeBuilder, Handle as RuntimeHandle, Runtime};
use tokio_timer::timer::Handle;

use super::{
    raft_client::{ConnectionBuilder, RaftClient},
    transport::ServerTransport,
};
use crate::{RaftKv, service::KvService};

const LOAD_STATISTICS_SLOTS: usize = 4;
const LOAD_STATISTICS_INTERVAL: Duration = Duration::from_millis(100);
const MEMORY_USAGE_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
pub const GRPC_THREAD_PREFIX: &str = "grpc-server";
pub const READPOOL_NORMAL_THREAD_PREFIX: &str = "store-read-norm";
pub const STATS_THREAD_PREFIX: &str = "transport-stats";

fn duration_to_i32_ms(duration: Duration) -> Option<i32> {
    let ms = duration.as_millis();
    if ms == 0 {
        return None;
    }
    Some(ms.min(i32::MAX as u128) as i32)
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0:?}")]
    Other(#[from] Box<dyn StdError + Sync + Send>),

    // Following is for From other errors.
    #[error("{0:?}")]
    Io(#[from] IoError),

    #[error("{0}")]
    Protobuf(#[from] ProtobufError),

    #[error("{0:?}")]
    Grpc(#[from] GrpcError),

    #[error("{0:?}")]
    Codec(#[from] CodecError),

    #[error("{0:?}")]
    AddrParse(#[from] AddrParseError),

    #[error("{0:?}")]
    RaftServer(Box<RaftServerError>),

    #[error("{0:?}")]
    Engine(#[from] EngineError),

    #[error("{0:?}")]
    EngineTrait(#[from] EngineTraitError),

    #[error("{0:?}")]
    Storage(#[from] StorageError),

    #[error("{0:?}")]
    Pd(#[from] PdError),

    #[error("failed to poll from mpsc receiver")]
    Sink,

    #[error("{0:?}")]
    RecvError(#[from] Canceled),

    #[error("{0:?}")]
    Http(#[from] HttpError),

    #[error("{0:?}")]
    OpenSsl(#[from] OpenSslError),

    #[error("{0:?}")]
    JsonCodec(#[from] serde_json::Error),

    #[error("{0:?}")]
    SecurityHttp(#[from] security::HttpClientError),
}

impl From<RaftServerError> for Error {
    fn from(e: RaftServerError) -> Self {
        Error::RaftServer(Box::new(e))
    }
}

pub type Result<T> = result::Result<T, Error>;

/// The TiKV server
///
/// It hosts various internal components, including gRPC, the raftstore router
/// and a snapshot worker.
pub struct Server<T: RaftStoreRouter + 'static, S: StoreAddrResolver + 'static> {
    env: Arc<Environment>,
    /// A GrpcServer builder or a GrpcServer.
    ///
    /// If the listening port is configured, the server will be started lazily.
    builder_or_server: Option<Either<ServerBuilder, GrpcServer>>,
    local_addr: SocketAddr,
    // Transport.
    trans: ServerTransport<T, S>,
    trans_idle: ServerTransport<T, S>,
    _raft_router: T,

    // Currently load statistics is done in the thread.
    stats_pool: Option<Runtime>,
    grpc_thread_load: Arc<ThreadLoadPool>,
    read_pool: Option<ReadPool>,
    debug_thread_pool: Arc<Runtime>,
    health_service: HealthService,
    timer: Handle,
}

impl<T: RaftStoreRouter + Unpin, S: StoreAddrResolver + 'static> Server<T, S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new<L: LockManager, F: KvFormat>(
        store_id: u64,
        cfg: &Arc<VersionTrack<Config>>,
        security_mgr: &Arc<SecurityManager>,
        storage: Storage<RaftKv, L, F>,
        copr: Endpoint<RaftKv>,
        raft_router: T,
        resolver: S,
        env: Arc<Environment>,
        read_pool: ReadPool,
        debug_thread_pool: Arc<Runtime>,
    ) -> Result<Self> {
        // A helper thread (or pool) for transport layer.
        let stats_pool = if cfg.value().stats_concurrency > 0 {
            Some(
                RuntimeBuilder::new_multi_thread()
                    .thread_name(STATS_THREAD_PREFIX)
                    .worker_threads(cfg.value().stats_concurrency)
                    .build()
                    .unwrap(),
            )
        } else {
            None
        };
        let grpc_thread_load = Arc::new(ThreadLoadPool::with_threshold(
            cfg.value().heavy_load_threshold,
        ));

        let proxy = Proxy::new(security_mgr.clone(), &env, Arc::new(cfg.value().clone()));
        let kv_service = KvService::new(
            store_id,
            storage,
            copr,
            raft_router.clone(),
            Arc::clone(&grpc_thread_load),
            cfg.value().enable_request_batch,
            proxy,
        );

        let addr = SocketAddr::from_str(&cfg.value().addr)?;
        let ip = format!("{}", addr.ip());
        let mem_quota = ResourceQuota::new(Some("ServerMemQuota"))
            .resize_memory(cfg.value().grpc_memory_pool_quota.0 as usize);
        let mut channel_builder = ChannelBuilder::new(Arc::clone(&env))
            .stream_initial_window_size(cfg.value().grpc_stream_initial_window_size.0 as i32)
            .max_concurrent_stream(cfg.value().grpc_concurrent_stream)
            .max_receive_message_len(-1)
            .set_resource_quota(mem_quota)
            .max_send_message_len(-1)
            .http2_max_ping_strikes(i32::MAX) // For pings without data from clients.
            .keepalive_time(cfg.value().grpc_keepalive_time.into())
            .keepalive_timeout(cfg.value().grpc_keepalive_timeout.into())
            .default_compression_algorithm(cfg.value().grpc_compression_algorithm())
            .default_gzip_compression_level(cfg.value().grpc_gzip_compression_level);
        if let Some(ms) = duration_to_i32_ms(cfg.value().grpc_connection_idle_time.0) {
            channel_builder = channel_builder
                .raw_cfg_int(CString::new("grpc.max_connection_idle_ms").unwrap(), ms);
        }
        let channel_args = channel_builder.build_args();
        let health_service = HealthService::default();
        let builder = {
            let mut sb = ServerBuilder::new(Arc::clone(&env))
                .channel_args(channel_args)
                .register_service(create_tikv(kv_service))
                .register_service(create_health(health_service.clone()));
            sb = security_mgr.bind(sb, &ip, addr.port());
            Either::Left(sb)
        };

        let conn_builder = ConnectionBuilder::new(
            env.clone(),
            cfg.clone(),
            security_mgr.clone(),
            resolver,
            raft_router.clone(),
            grpc_thread_load.clone(),
        );
        let raft_client = RaftClient::new(conn_builder.clone());
        let trans = ServerTransport::new(raft_client);

        let raft_idle_client = RaftClient::new(conn_builder);
        let trans_idle = ServerTransport::new(raft_idle_client);
        health_service.set_serving_status("", ServingStatus::NotServing);

        let svr = Server {
            env: Arc::clone(&env),
            builder_or_server: Some(builder),
            local_addr: addr,
            trans,
            trans_idle,
            _raft_router: raft_router,
            stats_pool,
            grpc_thread_load,
            read_pool: Some(read_pool),
            debug_thread_pool,
            health_service,
            timer: GLOBAL_TIMER_HANDLE.clone(),
        };

        Ok(svr)
    }

    pub fn get_debug_thread_pool(&self) -> &RuntimeHandle {
        self.debug_thread_pool.handle()
    }

    pub fn transport(&self) -> ServerTransport<T, S> {
        self.trans.clone()
    }

    pub fn transport_idle(&self) -> ServerTransport<T, S> {
        self.trans_idle.clone()
    }

    pub fn env(&self) -> Arc<Environment> {
        self.env.clone()
    }

    /// Register a gRPC service.
    /// Register after starting, it fails and returns the service.
    pub fn register_service(&mut self, svc: grpcio::Service) -> Option<grpcio::Service> {
        match self.builder_or_server.take() {
            Some(Either::Left(mut builder)) => {
                builder = builder.register_service(svc);
                self.builder_or_server = Some(Either::Left(builder));
                None
            }
            Some(server) => {
                self.builder_or_server = Some(server);
                Some(svc)
            }
            None => Some(svc),
        }
    }

    /// Build gRPC server and bind to address.
    pub fn build_and_bind(&mut self) -> Result<SocketAddr> {
        let sb = self.builder_or_server.take().unwrap().left().unwrap();
        let server = sb.build()?;
        let (host, port) = server.bind_addrs().next().unwrap();
        let addr = SocketAddr::new(IpAddr::from_str(host)?, port);
        self.local_addr = addr;
        self.builder_or_server = Some(Either::Right(server));
        Ok(addr)
    }

    /// Starts the TiKV server.
    /// Notice: Make sure call `build_and_bind` first.
    pub fn start(
        &mut self,
        _cfg: Arc<VersionTrack<Config>>,
        _security_mgr: Arc<SecurityManager>,
    ) -> Result<()> {
        let mut grpc_server = self.builder_or_server.take().unwrap().right().unwrap();
        info!("listening on addr"; "addr" => &self.local_addr);
        grpc_server.start();
        self.builder_or_server = Some(Either::Right(grpc_server));

        let mut grpc_load_stats = {
            let tl = Arc::clone(&self.grpc_thread_load);
            ThreadLoadStatistics::new(LOAD_STATISTICS_SLOTS, GRPC_THREAD_PREFIX, tl)
        };
        if let Some(ref p) = self.stats_pool {
            let mut delay = self
                .timer
                .interval(Instant::now(), LOAD_STATISTICS_INTERVAL)
                .compat();
            p.spawn(async move {
                while let Some(Ok(i)) = delay.next().await {
                    grpc_load_stats.record(i);
                }
            });
            let mut delay = self
                .timer
                .interval(Instant::now(), MEMORY_USAGE_REFRESH_INTERVAL)
                .compat();
            p.spawn(async move {
                while let Some(Ok(_)) = delay.next().await {
                    record_global_memory_usage();
                    MEMORY_USAGE_GAUGE.set(get_global_memory_usage() as i64);
                }
            });
        };

        let startup_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::Other(box_err!("Clock may have gone backwards")))?
            .as_secs();

        SERVER_INFO_GAUGE_VEC
            .with_label_values(&[
                &("v".to_owned() + tikv_build_version()),
                option_env!("TIKV_BUILD_GIT_HASH").unwrap_or("None"),
            ])
            .set(startup_ts as i64);
        self.health_service
            .set_serving_status("", ServingStatus::Serving);

        info!("TiKV is ready to serve");
        Ok(())
    }

    /// Stops the TiKV server.
    pub fn stop(&mut self) -> Result<()> {
        if let Some(Either::Right(mut server)) = self.builder_or_server.take() {
            server.shutdown();
        }
        if let Some(pool) = self.stats_pool.take() {
            pool.shutdown_background();
        }
        if let Some(read_pool) = self.read_pool.take() {
            read_pool.shutdown();
        }
        self.health_service
            .set_serving_status("", ServingStatus::NotServing);
        Ok(())
    }

    // Return listening address, this may only be used for outer test
    // to get the real address because we may use "127.0.0.1:0"
    // in test to avoid port conflict.
    pub fn listening_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

#[cfg(test)]
mod tests {
    use std::{ffi::CString, sync::Arc, time::Duration};

    use futures::executor::block_on;
    use grpcio::{ChannelBuilder, ConnectivityState, EnvBuilder, ServerBuilder};
    use grpcio_health::{HealthClient, HealthService, create_health, proto::HealthCheckRequest};

    use super::duration_to_i32_ms;

    #[test]
    fn test_grpc_connection_idle_closes() {
        let env = Arc::new(EnvBuilder::new().cq_count(1).build());
        let health_service = HealthService::default();
        health_service.set_serving_status("", grpcio_health::ServingStatus::Serving);

        let mut channel_builder = ChannelBuilder::new(env.clone());
        if let Some(ms) = duration_to_i32_ms(Duration::from_secs(1)) {
            channel_builder = channel_builder
                .raw_cfg_int(CString::new("grpc.max_connection_idle_ms").unwrap(), ms);
        }
        let channel_args = channel_builder.build_args();

        let mut server = ServerBuilder::new(env.clone())
            .channel_args(channel_args)
            .register_service(create_health(health_service.clone()))
            .bind("127.0.0.1", 0)
            .build()
            .unwrap();
        let addr = server
            .bind_addrs()
            .next()
            .map(|(host, port)| format!("{}:{}", host, port))
            .unwrap();
        server.start();

        let channel = ChannelBuilder::new(env).connect(&addr);
        let client = HealthClient::new(channel.clone());
        client.check(&HealthCheckRequest::default()).unwrap();

        assert!(block_on(channel.wait_for_connected(Duration::from_secs(3))));
        assert_eq!(
            channel.check_connectivity_state(false),
            ConnectivityState::GRPC_CHANNEL_READY
        );

        let changed = block_on(channel.wait_for_state_change(
            ConnectivityState::GRPC_CHANNEL_READY,
            Duration::from_secs(10),
        ));
        assert!(changed, "connection should change state after idle timeout");
        server.shutdown();
    }
}

#[cfg(any(test, feature = "testexport"))]
pub mod test_router {
    use std::sync::mpsc::*;

    use kvproto::raft_serverpb::RaftMessage;
    use rfstore::store::*;

    use super::*;

    #[derive(Clone)]
    pub struct TestRaftStoreRouter {
        tx: Sender<usize>,
        significant_msg_sender: Sender<SignificantMsg>,
    }

    impl TestRaftStoreRouter {
        pub fn new(
            tx: Sender<usize>,
            significant_msg_sender: Sender<SignificantMsg>,
        ) -> TestRaftStoreRouter {
            TestRaftStoreRouter {
                tx,
                significant_msg_sender,
            }
        }
    }

    impl StoreRouter for TestRaftStoreRouter {
        fn send(&self, _: StoreMsg) {
            let _ = self.tx.send(1);
        }
    }

    impl ProposalRouter for TestRaftStoreRouter {
        fn send(&self, _: RaftCommand) {
            let _ = self.tx.send(1);
        }
    }

    impl CasualRouter for TestRaftStoreRouter {
        fn send(&self, _: u64, _: CasualMessage) {
            let _ = self.tx.send(1);
        }
    }

    impl RaftStoreRouter for TestRaftStoreRouter {
        fn send_raft_msg(&self, _: RaftMessage) {
            let _ = self.tx.send(1);
        }

        fn significant_send(&self, _: u64, msg: SignificantMsg) {
            let _ = self.significant_msg_sender.send(msg);
        }
    }
}

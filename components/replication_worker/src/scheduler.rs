// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{borrow::Cow, collections::HashMap, sync::Arc, time::Duration};

use bytes::{Buf, Bytes};
use http::{HeaderMap, HeaderValue};
use hyper::{Body, Method, Request, Response, Result, StatusCode, Uri};
use merged_engine::ForceStop;
use security::HttpClient;
use serde::{Deserialize, Serialize};
use tikv_util::{box_err, future::paired_future_callback, info};

use crate::{CdcMsg, Error};

#[derive(Clone)]
pub struct ReplicationScheduler {
    sender: tikv_util::mpsc::Sender<CdcMsg>,
    cdc_addrs: Arc<dashmap::DashMap<u32, String>>,
    http_client: Arc<HttpClient>,
    scheme: String,
    #[allow(dead_code)]
    force_stop: ForceStop,
}

impl ReplicationScheduler {
    pub fn new(
        sender: tikv_util::mpsc::Sender<CdcMsg>,
        cdc_addrs: Arc<dashmap::DashMap<u32, String>>,
        http_client: Arc<HttpClient>,
        force_stop: ForceStop,
    ) -> Self {
        let scheme = if matches!(http_client.as_ref(), HttpClient::Http(_)) {
            "http".to_string()
        } else {
            "https".to_string()
        };
        ReplicationScheduler {
            sender,
            cdc_addrs,
            http_client,
            scheme,
            force_stop,
        }
    }

    pub fn schedule(&self, msg: CdcMsg) {
        let _ = self.sender.send(msg);
    }

    pub fn get_cdc_addr(&self, keyspace_id: u32) -> Option<String> {
        self.cdc_addrs
            .get(&keyspace_id)
            .map(|addr| addr.value().clone())
    }

    pub fn force_stop(&self) {
        #[cfg(feature = "testexport")]
        self.force_stop.set();
    }
}

#[derive(Serialize)]
struct ErrorResponse {
    error_msg: String,
    error_code: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct ChangefeedRequest {
    pub changefeed_id: String,
    pub sink_uri: String,
    #[serde(default)]
    pub start_ts: u64, // `0`: use current time.
}

#[derive(Serialize)]
#[allow(dead_code)]
struct ErrorInfo {
    addr: String,
    code: String,
    message: String,
}

#[derive(Serialize)]
#[allow(dead_code)]
struct TaskStatus {
    capture_id: String,
    table_ids: Vec<u64>,
}

pub async fn handle_cdc_request(
    scheduler: Option<&ReplicationScheduler>,
    req: Request<Body>,
) -> Result<Response<Body>> {
    if let Some(scheduler) = scheduler {
        scheduler.handle_http_request(req).await
    } else {
        Ok(Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .body("replication worker is disabled".into())
            .unwrap())
    }
}

impl ReplicationScheduler {
    fn error_response(status: StatusCode, msg: &str, code: &str) -> Response<Body> {
        let error = ErrorResponse {
            error_msg: msg.to_string(),
            error_code: code.to_string(),
        };
        Response::builder()
            .status(status)
            .body(Body::from(serde_json::to_string(&error).unwrap()))
            .unwrap()
    }

    pub async fn handle_http_request(&self, req: Request<Body>) -> Result<Response<Body>> {
        let (parts, body) = req.into_parts();
        let method = &parts.method;
        let uri = &parts.uri;
        let path = uri.path();
        let headers = parts.headers;

        if *method == Method::GET && path == "/cdc/keyspace" {
            return Ok(self.handle_get_keyspaces().await);
        }

        let query = uri.query().unwrap_or("");
        let query_pairs: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).collect();

        let keyspace_id = {
            if let Some(keyspace_str) = query_pairs.get("keyspace_id").map(|v| v.as_ref()) {
                match keyspace_str.parse::<u32>() {
                    Ok(keyspace_id) => keyspace_id,
                    Err(_) => {
                        return Ok(Self::error_response(
                            StatusCode::BAD_REQUEST,
                            "Invalid keyspace_id",
                            "CDC:ErrInvalidParam",
                        ));
                    }
                }
            } else {
                return Ok(Self::error_response(
                    StatusCode::BAD_REQUEST,
                    "keyspace_id is required",
                    "CDC:ErrInvalidParam",
                ));
            }
        };

        let response = match (method, path) {
            (&Method::POST, "/cdc/api/v2/changefeeds") => {
                let body_bytes = hyper::body::to_bytes(body).await?;
                self.handle_create_changefeed(keyspace_id, body_bytes).await
            }
            (&Method::POST, "/cdc/keyspace") => {
                let body_bytes = hyper::body::to_bytes(body).await?;
                self.handle_add_keyspace(keyspace_id, &body_bytes).await
            }
            (&Method::DELETE, "/cdc/keyspace") => {
                let force = match extract_bool_param(&query_pairs, "force") {
                    Ok(force) => force,
                    Err(err) => return Ok(err),
                };
                self.handle_remove_keyspace(keyspace_id, force).await
            }
            (&Method::DELETE, path) if path.starts_with("/cdc/api/v2/changefeeds/") => {
                self.handle_delete_changefeed(keyspace_id, path).await?
            }
            (method, path) if path.starts_with("/cdc/") => {
                self.forward_request_to_cdc(keyspace_id, method, uri, headers, body)
                    .await?
            }
            _ => {
                info!("Invalid request {} {}", method, uri);
                Self::error_response(
                    StatusCode::NOT_FOUND,
                    "Route not found",
                    "CDC:ErrAPIRouteNotFound",
                )
            }
        };

        Ok(response)
    }

    async fn handle_delete_changefeed(
        &self,
        keyspace_id: u32,
        path: &str,
    ) -> Result<Response<Body>> {
        if let Some(changefeed_id) = path.strip_prefix("/cdc/api/v2/changefeeds/") {
            let cdc_addr = self.get_cdc_addr(keyspace_id);
            if cdc_addr.is_none() {
                return Ok(Self::error_response(
                    StatusCode::NOT_FOUND,
                    "No CDC server found for the keyspace",
                    "CDC:ErrNoCDCServer",
                ));
            }
            let cdc_addr = cdc_addr.unwrap();
            let remove_cdc_task_uri = format!(
                "{}://{}/api/v2/changefeeds/{}",
                &self.scheme, &cdc_addr, changefeed_id
            );

            let req = http::Request::delete(remove_cdc_task_uri)
                .body(hyper::Body::empty())
                .unwrap();
            self.http_client.request(req).await?;
            let (cb, fut) = paired_future_callback();
            self.schedule(CdcMsg::RemoveTask {
                keyspace_id,
                changefeed_id: changefeed_id.to_string(),
                cb,
            });
            let res = fut.await.unwrap();
            if let Err(err) = res {
                return Ok(Self::error_response(
                    StatusCode::BAD_REQUEST,
                    &err.to_string(),
                    "CDC:ErrRemoveChangefeed",
                ));
            }
            Ok(Response::builder()
                .status(StatusCode::OK)
                .body(Body::empty())
                .unwrap())
        } else {
            Ok(Self::error_response(
                StatusCode::BAD_REQUEST,
                "Invalid changefeed ID",
                "CDC:ErrInvalidChangefeedID",
            ))
        }
    }

    async fn forward_request_to_cdc(
        &self,
        keyspace_id: u32,
        method: &Method,
        uri: &Uri,
        headers: HeaderMap<HeaderValue>,
        body: Body,
    ) -> Result<Response<Body>> {
        let cdc_addr = self.cdc_addrs.get(&keyspace_id).map(|r| r.value().clone());
        if cdc_addr.is_none() {
            return Ok(Self::error_response(
                StatusCode::NOT_FOUND,
                "No CDC server found for the keyspace",
                "CDC:ErrNoCDCServer",
            ));
        }
        let new_path_query = uri
            .path_and_query()
            .unwrap()
            .to_string()
            .replace("/cdc/", "/");
        let cdc_addr = cdc_addr.unwrap();
        let new_uri = Uri::builder()
            .scheme(self.scheme.as_str())
            .authority(cdc_addr.as_str())
            .path_and_query(new_path_query)
            .build()
            .unwrap();

        let mut req_builder = Request::builder().method(method).uri(new_uri);

        for (key, value) in headers.iter() {
            req_builder = req_builder.header(key, value);
        }

        let req = req_builder.body(body).unwrap();

        self.http_client.request(req).await
    }

    async fn handle_create_changefeed(&self, keyspace_id: u32, body: Bytes) -> Response<Body> {
        info!("handle create change feed");
        match serde_json::from_slice::<ChangefeedRequest>(body.chunk()) {
            Ok(request) => {
                if request.changefeed_id.is_empty() {
                    return Self::error_response(
                        StatusCode::BAD_REQUEST,
                        "changefeed_id is required",
                        "CDC:ErrInvalidRequestBody",
                    );
                }
                if request.sink_uri.is_empty() {
                    return Self::error_response(
                        StatusCode::BAD_REQUEST,
                        "sink_uri is required",
                        "CDC:ErrInvalidRequestBody",
                    );
                }
                let (cb, fut) = paired_future_callback();

                self.schedule(CdcMsg::NewTask {
                    keyspace_id,
                    changefeed_id: request.changefeed_id,
                    start_ts: request.start_ts,
                    body,
                    cb,
                });
                let res = fut.await.unwrap();
                if let Err(err) = res {
                    return Self::error_response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        &err.to_string(),
                        "CDC:ErrCreateChangefeed",
                    );
                }
                let (status, body) = res.unwrap();
                Response::builder()
                    .status(status)
                    .body(body.into())
                    .unwrap()
            }
            Err(e) => Self::error_response(
                StatusCode::BAD_REQUEST,
                &e.to_string(),
                "CDC:ErrInvalidRequestBody",
            ),
        }
    }

    async fn handle_add_keyspace(&self, keyspace_id: u32, body_bytes: &[u8]) -> Response<Body> {
        info!("handle add keyspace"; "keyspace" => keyspace_id);
        let (cb, fut) = paired_future_callback();
        let msg = match serde_json::from_slice::<crate::scheduler::ProvisionedKeyspace>(body_bytes)
        {
            Ok(provisioned) => CdcMsg::AddKeyspace {
                keyspace_id,
                pd_url: provisioned.pd_url,
                cdc_addr: provisioned.cdc_addr,
                cb,
            },
            Err(_err) => CdcMsg::AddKeyspace {
                keyspace_id,
                pd_url: "".into(),
                cdc_addr: "".into(),
                cb,
            },
        };
        self.schedule(msg);
        let res = fut.await.unwrap();
        if let Err(err) = res {
            return Self::error_response(
                StatusCode::BAD_REQUEST,
                &err.to_string(),
                "CDC:ErrAddKeyspace",
            );
        }
        Response::builder()
            .status(StatusCode::OK)
            .body(Body::empty())
            .unwrap()
    }

    async fn handle_remove_keyspace(&self, keyspace_id: u32, force: bool) -> Response<Body> {
        info!("handle remove keyspace"; "keyspace" => keyspace_id, "force" => force);
        let (cb, fut) = paired_future_callback();
        self.schedule(CdcMsg::RemoveKeyspace {
            keyspace_id,
            force,
            cb,
        });
        let res = fut.await.unwrap();
        if let Err(err) = res {
            return Self::error_response(
                StatusCode::BAD_REQUEST,
                &err.to_string(),
                "CDC:ErrRemoveKeyspace",
            );
        }
        Response::builder()
            .status(StatusCode::OK)
            .body(Body::empty())
            .unwrap()
    }

    async fn handle_get_keyspaces(&self) -> Response<Body> {
        info!("handle get keyspaces");
        let (cb, fut) = paired_future_callback();
        self.schedule(CdcMsg::GetKeyspaces { cb });
        let res = fut.await.unwrap();
        let keyspaces_resp = KeyspacesResp { keyspace_ids: res };
        let res_str = serde_json::to_string(&keyspaces_resp).unwrap();
        Response::builder()
            .status(StatusCode::OK)
            .body(res_str.into())
            .unwrap()
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Default)]
#[serde(default)]
pub struct ProvisionedKeyspace {
    pub pd_url: String,
    pub cdc_addr: String,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Default)]
#[serde(default)]
pub struct KeyspacesResp {
    pub keyspace_ids: Vec<u32>,
}

fn extract_bool_param(
    query_pairs: &HashMap<Cow<'_, str>, Cow<'_, str>>,
    key: &str,
) -> std::result::Result<bool, Response<Body>> {
    match query_pairs.get(key).map(|v| v.as_ref()) {
        None => Ok(false),
        Some("") | Some("0") | Some("false") => Ok(false),
        Some("1") | Some("true") => Ok(true),
        Some(_) => Err(ReplicationScheduler::error_response(
            StatusCode::BAD_REQUEST,
            &format!("Invalid boolean param \"{key}\""),
            "CDC:ErrInvalidParam",
        )),
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Default)]
#[serde(default)]
pub struct CdcStatus {
    pub liveness: usize,
}

pub(crate) async fn get_cdc_status(
    client: &HttpClient,
    cdc_addr: &str,
    timeout: Duration,
) -> crate::Result<()> {
    let scheme = if matches!(client, HttpClient::Http(_)) {
        "http"
    } else {
        "https"
    };
    let url = format!("{}://{}/api/v2/status", scheme, cdc_addr);
    let start = tikv_util::time::Instant::now();
    while start.saturating_elapsed() < timeout {
        let uri = Uri::try_from(&url).unwrap();
        let resp = client.get(uri).await;
        let Ok(resp) = resp else {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        };
        let resp_data = hyper::body::to_bytes(resp.into_body()).await?;
        let status = serde_json::from_slice::<CdcStatus>(&resp_data)
            .map_err(|e| -> Error { box_err!("Failed to parse CDC status: {}", e) })?;
        if status.liveness == 0 {
            return Ok(());
        }
    }
    Err(box_err!("CDC status check timed out"))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use hyper::http::request;

    use super::*;

    const VALID_CHANGEFEED_STATES: [&str; 6] =
        ["all", "normal", "stopped", "error", "failed", "finished"];

    fn parse_query(uri: &Uri) -> HashMap<Cow<'_, str>, Cow<'_, str>> {
        let query = uri.query().unwrap_or("");
        url::form_urlencoded::parse(query.as_bytes()).collect()
    }

    fn test_cdc_addr() -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let make_svc = hyper::service::make_service_fn(|_| async {
            Ok::<_, hyper::Error>(hyper::service::service_fn(|req| async move {
                let path = req.uri().path();
                let query_pairs = parse_query(req.uri());
                let status = if let Some(suffix) = path.strip_prefix("/api/v2/changefeeds/") {
                    if suffix.is_empty() {
                        StatusCode::BAD_REQUEST
                    } else {
                        StatusCode::OK
                    }
                } else if path == "/api/v2/changefeeds" {
                    if let Some(state) = query_pairs.get("state")
                        && !VALID_CHANGEFEED_STATES.contains(&state.as_ref())
                    {
                        StatusCode::BAD_REQUEST
                    } else {
                        StatusCode::OK
                    }
                } else {
                    StatusCode::NOT_FOUND
                };
                Ok::<_, hyper::Error>(
                    Response::builder()
                        .status(status)
                        .body(Body::empty())
                        .unwrap(),
                )
            }))
        });
        let server = hyper::Server::from_tcp(listener).unwrap().serve(make_svc);
        tokio::spawn(async move {
            let _ = server.await;
        });
        addr.to_string()
    }

    impl ReplicationScheduler {
        fn new_test() -> Self {
            let (sender, receiver) = tikv_util::mpsc::unbounded();
            std::thread::spawn(move || {
                while let Ok(msg) = receiver.recv() {
                    match msg {
                        CdcMsg::NewTask { cb, .. } => {
                            cb(Ok((StatusCode::OK, Bytes::from_static(b"{}"))));
                        }
                        CdcMsg::RemoveTask { cb, .. }
                        | CdcMsg::RemoveKeyspace { cb, .. }
                        | CdcMsg::AddKeyspace { cb, .. }
                        | CdcMsg::LoadKeyspaceShards { cb, .. } => {
                            cb(Ok(()));
                        }
                        CdcMsg::GetKeyspaces { cb } => {
                            cb(Vec::new());
                        }
                        CdcMsg::LoadKeyspaceShardMetas { cb, .. } => {
                            cb(Ok(std::collections::HashMap::new()));
                        }
                        _ => {}
                    }
                }
            });
            let cdc_addrs = Arc::new(dashmap::DashMap::new());
            cdc_addrs.insert(1, test_cdc_addr());
            ReplicationScheduler::new(
                sender,
                cdc_addrs,
                Arc::new(HttpClient::Http(hyper::Client::new())),
                ForceStop::default(),
            )
        }
    }

    #[tokio::test]
    async fn test_create_changefeed() {
        let worker = ReplicationScheduler::new_test();
        let req = request::Builder::new()
            .method(Method::POST)
            .uri("/cdc/api/v2/changefeeds?keyspace_id=1")
            .body(Body::from(
                r#"{
                    "changefeed_id": "test1",
                    "sink_uri": "blackhole://"
                }"#,
            ))
            .unwrap();

        let resp = worker.handle_http_request(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_list_changefeeds() {
        let worker = ReplicationScheduler::new_test();
        let req = request::Builder::new()
            .method(Method::GET)
            .uri("/cdc/api/v2/changefeeds?keyspace_id=1&state=normal")
            .body(Body::empty())
            .unwrap();

        let resp = worker.handle_http_request(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_changefeed() {
        let worker = ReplicationScheduler::new_test();
        let req = request::Builder::new()
            .method(Method::GET)
            .uri("/cdc/api/v2/changefeeds/test1?keyspace_id=1")
            .body(Body::empty())
            .unwrap();

        let resp = worker.handle_http_request(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_delete_changefeed() {
        let worker = ReplicationScheduler::new_test();
        let req = request::Builder::new()
            .method(Method::DELETE)
            .uri("/cdc/api/v2/changefeeds/test1?keyspace_id=1")
            .body(Body::empty())
            .unwrap();

        let resp = worker.handle_http_request(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_invalid_request() {
        let worker = ReplicationScheduler::new_test();
        // Missing keyspace_id for list changefeeds
        let req = request::Builder::new()
            .method(Method::GET)
            .uri("/cdc/api/v2/changefeeds")
            .body(Body::empty())
            .unwrap();
        let resp = worker.handle_http_request(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        // Missing keyspace_id for get changefeed
        let req = request::Builder::new()
            .method(Method::GET)
            .uri("/cdc/api/v2/changefeeds/test1")
            .body(Body::empty())
            .unwrap();
        let resp = worker.handle_http_request(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        // Missing keyspace_id for create changefeed
        let req = request::Builder::new()
            .method(Method::POST)
            .uri("/cdc/api/v2/changefeeds")
            .body(Body::from(
                r#"{
                "changefeed_id": "test1",
                "sink_uri": "blackhole://"
            }"#,
            ))
            .unwrap();
        let resp = worker.handle_http_request(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        // Missing keyspace_id for delete changefeed
        let req = request::Builder::new()
            .method(Method::DELETE)
            .uri("/cdc/api/v2/changefeeds/test1")
            .body(Body::empty())
            .unwrap();
        let resp = worker.handle_http_request(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        // Invalid state parameter
        let req = request::Builder::new()
            .method(Method::GET)
            .uri("/cdc/api/v2/changefeeds?keyspace_id=1&state=invalid")
            .body(Body::empty())
            .unwrap();
        let resp = worker.handle_http_request(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        // Invalid changefeed ID format
        let req = request::Builder::new()
            .method(Method::GET)
            .uri("/cdc/api/v2/changefeeds/?keyspace_id=1")
            .body(Body::empty())
            .unwrap();
        let resp = worker.handle_http_request(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        // Missing required field in create request
        let req = request::Builder::new()
            .method(Method::POST)
            .uri("/cdc/api/v2/changefeeds?keyspace_id=1")
            .body(Body::from("{}"))
            .unwrap();
        let resp = worker.handle_http_request(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_extract_bool_param() {
        let cases = vec![
            ("/cdc/keyspace?keyspace_id=1", Ok(false)),
            ("/cdc/keyspace?keyspace_id=1&force=1", Ok(true)),
            ("/cdc/keyspace?keyspace_id=1&force=true", Ok(true)),
            ("/cdc/keyspace?keyspace_id=1&force=0", Ok(false)),
            ("/cdc/keyspace?keyspace_id=1&force=false", Ok(false)),
            (
                "/cdc/keyspace?keyspace_id=1&force=maybe",
                Err(StatusCode::BAD_REQUEST),
            ),
        ];

        for (uri, expected) in cases {
            let uri = Uri::from_static(uri);
            let query_pairs = parse_query(&uri);
            match expected {
                Ok(value) => assert_eq!(extract_bool_param(&query_pairs, "force").unwrap(), value),
                Err(status) => {
                    let err_resp = extract_bool_param(&query_pairs, "force").unwrap_err();
                    assert_eq!(err_resp.status(), status);
                }
            }
        }
    }
}

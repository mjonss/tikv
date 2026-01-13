// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

// TODO(iosmanthus): introduce TLS support for the whole integration test.

use std::{
    convert::TryFrom,
    error::Error as StdError,
    fs, io, iter,
    result::Result as StdResult,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use bstr::ByteSlice;
use bytes::Bytes;
use http::{Method, Request, StatusCode};
use hyper::{Body, Uri, client::HttpConnector, server::conn::AddrIncoming};
use hyper_rustls::{HttpsConnector, TlsAcceptor};
use rustls::{
    SignatureScheme,
    client::ResolvesClientCert,
    server::{AllowAnyAnonymousOrAuthenticatedClient, ClientHello, ResolvesServerCert},
    sign::{CertifiedKey, any_supported_type},
};
use rustls_pemfile::Item;
use tikv_util::{Either, debug, error, time::Instant};

use crate::{SecurityConfig, SecurityManager, metrics::HYPER_RELOAD_CERT_COUNTER};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("HTTP error: {0}:{1}")]
    Http(StatusCode, String),
    #[error("request timeout: {0}")]
    Timeout(String),
    #[error(transparent)]
    Hyper(#[from] hyper::Error),
    #[error(transparent)]
    InvalidUri(#[from] http::uri::InvalidUri),
    #[error(transparent)]
    Tls(#[from] rustls::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Other(#[from] Box<dyn StdError + Sync + Send>),
}

pub type Result<T> = std::result::Result<T, Error>;

impl SecurityManager {
    pub fn acceptor(&self, incoming: AddrIncoming) -> Result<Either<AddrIncoming, TlsAcceptor>> {
        if self.cfg.ca_path.is_empty() {
            Ok(Either::Left(incoming))
        } else {
            Ok(Either::Right(self.tls_acceptor(incoming)?))
        }
    }

    pub fn build_uri<T: AsRef<str>>(&self, rest: T) -> Result<Uri> {
        let scheme = if self.cfg.ca_path.is_empty() {
            "http"
        } else {
            "https"
        };
        Ok(Uri::try_from(format!("{}://{}", scheme, rest.as_ref()))?)
    }

    fn tls_acceptor(&self, incoming: AddrIncoming) -> Result<TlsAcceptor> {
        let ca = load_root_store(&self.cfg.ca_path)?;
        let cert_resolver = ReloadWhenChanged::new(self.cfg.clone())?;

        let tls_config = rustls::ServerConfig::builder()
            .with_safe_defaults()
            .with_client_cert_verifier(AllowAnyAnonymousOrAuthenticatedClient::new(ca).boxed())
            .with_cert_resolver(cert_resolver);

        let acceptor = TlsAcceptor::builder()
            .with_tls_config(tls_config)
            .with_alpn_protocols(vec![b"http/1.1".to_vec(), b"h2".to_vec()])
            .with_incoming(incoming);

        Ok(acceptor)
    }

    pub fn http_client(&self, client_builder: hyper::client::Builder) -> Result<HttpClient> {
        if self.cfg.ca_path.is_empty() {
            Ok(HttpClient::Http(hyper::Client::new()))
        } else {
            let ca = load_root_store(&self.cfg.ca_path)?;
            let cert_resolver = ReloadWhenChanged::new(self.cfg.clone())?;
            let tls_config = rustls::ClientConfig::builder()
                .with_safe_defaults()
                .with_root_certificates(ca)
                .with_client_cert_resolver(cert_resolver);

            let connector = hyper_rustls::HttpsConnectorBuilder::new()
                .with_tls_config(tls_config)
                .https_only()
                .enable_http1()
                .build();
            Ok(HttpClient::Https(client_builder.build(connector)))
        }
    }
}

struct ReloadWhenChanged {
    cfg: Arc<SecurityConfig>,
    latest: Mutex<CertState>,
}

struct CertState {
    key: Arc<CertifiedKey>,
    atime: SystemTime,
    mtime: SystemTime,
}

#[derive(Debug)]
enum CertType {
    Server,
    Client,
}

impl ReloadWhenChanged {
    fn new(cfg: Arc<SecurityConfig>) -> StdResult<Arc<Self>, Box<dyn StdError + Sync + Send>> {
        let mtime = fs::metadata(&cfg.cert_path)?.modified()?;
        Ok(Arc::new(Self {
            cfg: cfg.clone(),
            latest: Mutex::new(CertState {
                key: Self::load_certified_key(&cfg)?,
                atime: SystemTime::now(),
                mtime,
            }),
        }))
    }

    fn load_certified_key(
        cfg: &SecurityConfig,
    ) -> std::result::Result<Arc<CertifiedKey>, Box<dyn StdError + Sync + Send>> {
        let cert = load_certs(&cfg.cert_path)?;
        let key = load_key(&cfg.key_path)?;
        Ok(Arc::new(CertifiedKey::new(cert, any_supported_type(&key)?)))
    }

    fn resolve_impl(&self, cert_type: CertType) -> Arc<CertifiedKey> {
        let mut latest = self.latest.lock().unwrap();
        let old_key = latest.key.clone();
        if SystemTime::now()
            .duration_since(latest.atime)
            .unwrap_or_default()
            < self.cfg.cert_reload_interval.0
        {
            return old_key;
        }

        // Update the access time of the cert state.
        latest.atime = SystemTime::now();
        let mut mtime = Some(latest.mtime);
        match self.cfg.is_modified(&mut mtime) {
            Ok(false) => {
                return old_key;
            }
            Err(e) => {
                HYPER_RELOAD_CERT_COUNTER
                    .with_label_values(&["fail", &format!("{:?}", cert_type)])
                    .inc();
                error!( "fail to check mtime of certificates"; "err"=>?e, "cert_type"=>?cert_type);
                return old_key;
            }
            _ => {}
        }

        match Self::load_certified_key(&self.cfg) {
            Ok(new_key) => {
                latest.key = new_key;
                latest.mtime = mtime.unwrap();
                HYPER_RELOAD_CERT_COUNTER
                    .with_label_values(&["success", &format!("{:?}", cert_type)])
                    .inc();
                latest.key.clone()
            }
            Err(e) => {
                HYPER_RELOAD_CERT_COUNTER
                    .with_label_values(&["fail", &format!("{:?}", cert_type)])
                    .inc();
                error!("fail to load certificates";"err"=>?e,"cert_type"=>?cert_type);
                old_key
            }
        }
    }
}

impl ResolvesServerCert for ReloadWhenChanged {
    fn resolve(&self, _: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.resolve_impl(CertType::Server))
    }
}

impl ResolvesClientCert for ReloadWhenChanged {
    fn resolve(
        &self,
        _acceptable_issuers: &[&[u8]],
        _sigschemes: &[SignatureScheme],
    ) -> Option<Arc<CertifiedKey>> {
        Some(self.resolve_impl(CertType::Client))
    }

    fn has_certs(&self) -> bool {
        true
    }
}

fn error<E>(err: E) -> io::Error
where
    E: Into<Box<dyn StdError + Send + Sync>>,
{
    io::Error::new(io::ErrorKind::Other, err)
}

fn load_certs(filename: &str) -> io::Result<Vec<rustls::Certificate>> {
    let certfile = fs::File::open(filename)?;
    let mut reader = io::BufReader::new(certfile);
    let certs = rustls_pemfile::certs(&mut reader)?;
    Ok(certs.into_iter().map(rustls::Certificate).collect())
}

fn load_key(filename: &str) -> io::Result<rustls::PrivateKey> {
    let keyfile = fs::File::open(filename)?;
    let mut reader = io::BufReader::new(keyfile);
    if let Some(item) = iter::from_fn(|| rustls_pemfile::read_one(&mut reader).transpose()).next() {
        let key = match item? {
            Item::RSAKey(key) => key,
            Item::PKCS8Key(key) => key,
            Item::ECKey(key) => key,
            _ => return Err(error("unsupported private key type")),
        };
        return Ok(rustls::PrivateKey(key));
    }
    Err(error("no private keys found"))
}

fn load_root_store(filename: &str) -> io::Result<rustls::RootCertStore> {
    let ca_certs = load_certs(filename)?;
    let mut store = rustls::RootCertStore::empty();
    for cert in ca_certs.iter() {
        store.add(cert).map_err(error)?;
    }
    Ok(store)
}

#[derive(Clone)]
pub enum HttpClient {
    Http(hyper::Client<HttpConnector>),
    Https(hyper::Client<HttpsConnector<HttpConnector>>),
}

impl HttpClient {
    pub fn get(&self, uri: Uri) -> hyper::client::ResponseFuture {
        match self {
            HttpClient::Http(client) => client.get(uri),
            HttpClient::Https(client) => client.get(uri),
        }
    }

    pub fn request(&self, req: hyper::http::Request<hyper::Body>) -> hyper::client::ResponseFuture {
        match self {
            HttpClient::Http(client) => client.request(req),
            HttpClient::Https(client) => client.request(req),
        }
    }
}

pub struct RestfulClient {
    tag: String,
    security_mgr: Arc<SecurityManager>,
    endpoints: Vec<String>,
    last_endpoint_idx: Option<AtomicUsize>,
    req_timeout: Duration,
    retry_timeout: Duration,
    retry_on_server_error: bool,
}

impl Clone for RestfulClient {
    fn clone(&self) -> Self {
        Self {
            tag: self.tag.clone(),
            security_mgr: self.security_mgr.clone(),
            endpoints: self.endpoints.clone(),
            last_endpoint_idx: self
                .last_endpoint_idx
                .as_ref()
                .map(|idx| AtomicUsize::new(idx.load(Ordering::Relaxed))),
            req_timeout: self.req_timeout,
            retry_timeout: self.retry_timeout,
            retry_on_server_error: self.retry_on_server_error,
        }
    }
}

impl RestfulClient {
    pub fn new(
        tag: impl AsRef<str>,
        endpoints: Vec<String>,
        security_mgr: Arc<SecurityManager>,
    ) -> Result<Self> {
        let endpoint_idx = (endpoints.len() > 1).then(|| AtomicUsize::new(0));
        Ok(Self {
            tag: tag.as_ref().to_owned(),
            endpoints: Self::trim_schema(endpoints),
            security_mgr,
            last_endpoint_idx: endpoint_idx,
            req_timeout: Duration::from_secs(3),
            retry_timeout: Duration::from_secs(6),
            retry_on_server_error: true,
        })
    }

    pub fn set_timeout(&mut self, req_timeout: Duration, retry_timeout: Duration) {
        self.req_timeout = req_timeout;
        self.retry_timeout = retry_timeout;
    }

    pub fn set_retry_on_server_error(&mut self, retry: bool) {
        self.retry_on_server_error = retry;
    }

    pub async fn request(
        &self,
        path: impl AsRef<str>,
        method: Method,
        body_data: Option<Bytes>,
    ) -> Result<Bytes> {
        let path = path.as_ref();
        let client = self.security_mgr.http_client(hyper::Client::builder())?;
        let mut err = None;
        let last_ep_idx = self.last_endpoint_idx();
        let mut idx = 0;
        let start_time = Instant::now_coarse();
        while start_time.saturating_elapsed() < self.retry_timeout {
            let current_ep_idx = (last_ep_idx + idx) % self.endpoints.len();
            idx += 1;
            let endpoint = &self.endpoints[current_ep_idx];

            let uri = self.security_mgr.build_uri(format!("{endpoint}/{path}"))?;
            let tag = format!("{}:{}:{}", self.tag, method.as_str(), uri);
            let req = Request::builder()
                .method(method.clone())
                .uri(uri)
                .body(match body_data {
                    Some(ref data) => Body::from(data.clone()),
                    None => Body::empty(),
                })
                .unwrap();
            let resp = tokio::time::timeout(self.req_timeout, client.request(req)).await;
            match resp {
                Err(_elapsed) => err = Some(Error::Timeout(format!("{}: request timeout", tag))),
                Ok(Err(e)) => err = Some(Error::Hyper(e)),
                Ok(Ok(resp)) => {
                    let status = resp.status();
                    let body_res = tokio::time::timeout(
                        self.req_timeout,
                        hyper::body::to_bytes(resp.into_body()),
                    )
                    .await;
                    if status.is_success() {
                        let Ok(body) = body_res else {
                            err = Some(Error::Timeout(format!("{}: read body timeout", tag)));
                            continue;
                        };
                        let body = body.unwrap();
                        debug!("{}: success: {}", tag, body.to_str_lossy());
                        if current_ep_idx != last_ep_idx {
                            self.set_last_endpoint_idx(current_ep_idx);
                        }
                        return Ok(body);
                    } else {
                        let body = body_res.map(|body| body.unwrap()).unwrap_or_default();
                        let msg = body.to_str_lossy();
                        error!("{}: http client error: {}: {}", tag, status, msg.as_ref());
                        let this_err = Error::Http(status, msg.to_string());
                        if status.is_client_error() || !self.retry_on_server_error {
                            return Err(this_err);
                        } else {
                            err = Some(this_err);
                        }
                    }
                }
            }
        }
        Err(err.expect("there must be error"))
    }

    pub async fn get<T>(&self, path: impl AsRef<str>) -> Result<T>
    where
        T: std::fmt::Debug + for<'a> serde::de::Deserialize<'a>,
    {
        let path = path.as_ref();
        match self.request(path, Method::GET, None).await {
            Ok(resp) => {
                let t: T = serde_json::from_slice(&resp)?;
                debug!("{}: get", self.tag; "path" => path, "resp" => ?t);
                Ok(t)
            }
            Err(err) => Err(err),
        }
    }

    pub async fn post<Req, Resp>(&self, path: impl AsRef<str>, data: &Req) -> Result<Resp>
    where
        Req: serde::ser::Serialize,
        Resp: std::fmt::Debug + for<'a> serde::de::Deserialize<'a>,
    {
        self.post_or_put(Method::POST, path, data).await
    }

    pub async fn put<Req, Resp>(&self, path: impl AsRef<str>, data: &Req) -> Result<Resp>
    where
        Req: serde::ser::Serialize,
        Resp: std::fmt::Debug + for<'a> serde::de::Deserialize<'a>,
    {
        self.post_or_put(Method::PUT, path, data).await
    }

    async fn post_or_put<Req, Resp>(
        &self,
        method: Method,
        path: impl AsRef<str>,
        data: &Req,
    ) -> Result<Resp>
    where
        Req: serde::ser::Serialize,
        Resp: std::fmt::Debug + for<'a> serde::de::Deserialize<'a>,
    {
        let path = path.as_ref();
        let body_data = Bytes::from(serde_json::to_vec(data)?);
        match self.request(path, method, Some(body_data)).await {
            Ok(resp) => {
                let t: Resp = serde_json::from_slice(&resp)?;
                debug!("{}: post", self.tag; "path" => path, "resp" => ?t);
                Ok(t)
            }
            Err(err) => Err(err),
        }
    }

    pub async fn delete(&self, path: impl AsRef<str>) -> Result<Bytes> {
        let path = path.as_ref();
        match self.request(path, Method::DELETE, None).await {
            Ok(resp) => {
                debug!("{}: delete", self.tag; "path" => path, "resp" => resp.to_str_lossy().as_ref());
                Ok(resp)
            }
            Err(err) => Err(err),
        }
    }

    fn trim_schema(mut endpoints: Vec<String>) -> Vec<String> {
        endpoints.iter_mut().for_each(|endpoint| {
            for prefix in &["http://", "https://"] {
                if endpoint.starts_with(prefix) {
                    endpoint.replace_range(0..prefix.len(), "");
                    break;
                }
            }
        });
        endpoints
    }

    fn last_endpoint_idx(&self) -> usize {
        self.last_endpoint_idx
            .as_ref()
            .map_or(0, |idx| idx.load(Ordering::Relaxed))
    }

    fn set_last_endpoint_idx(&self, ep_idx: usize) {
        if let Some(idx) = self.last_endpoint_idx.as_ref() {
            idx.store(ep_idx, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_restful_client() {
        let endpoints = vec![
            "127.0.0.1".to_string(),
            "http://127.0.0.2:4379".to_string(),
            "https://127.0.0.3:4380".to_string(),
        ];
        let endpoints = RestfulClient::trim_schema(endpoints);
        assert_eq!(
            endpoints,
            vec![
                "127.0.0.1".to_string(),
                "127.0.0.2:4379".to_string(),
                "127.0.0.3:4380".to_string(),
            ]
        );
    }
}

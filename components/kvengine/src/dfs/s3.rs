// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{Debug, Formatter},
    io::Write,
    ops::{Deref, DerefMut},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use bstr::ByteSlice;
use bytes::{BufMut, Bytes, BytesMut};
use engine_traits::{GetObjectOptions, ListObjectContent, ObjectCacheWithHook, ObjectStorage};
use fail::fail_point;
use futures::StreamExt;
use http::StatusCode;
use hyper_tls::HttpsConnector;
use rusoto_core::{
    HttpClient, HttpDispatchError, Region, RusotoError,
    param::{Params, ServiceParams},
    request::{BufferedHttpResponse, HttpResponse},
    signature::SignedRequest,
};
use rusoto_s3::{
    CopyObjectError, DeleteObjectError, GetObjectError, GetObjectTaggingError, HeadObjectError,
    ListObjectsV2Error, PutObjectError,
};
use tikv_util::{box_join_err, errors::BoxError, time::Instant};
use tokio::runtime::Runtime;

use crate::dfs::{
    self, Dfs, Error, Options, ReservableWriter,
    config::{Config, ConnOptions},
    metrics::*,
};

pub const STORAGE_CLASS_DEFAULT: &str = STORAGE_CLASS_INTELLIGENT_TIERING;
pub const STORAGE_CLASS_INTELLIGENT_TIERING: &str = "INTELLIGENT_TIERING";
pub const STORAGE_CLASS_STANDARD: &str = "STANDARD";
pub const STORAGE_CLASS_STANDARD_IA: &str = "STANDARD_IA";
pub const STORAGE_CLASS_GLACIER_IR: &str = "GLACIER_IR";

// In Amazon S3, the Standard storage class is referred to as STANDARD, the
// Infrequent Access (IA) storage class is referred to as STANDARD_IA, and the
// Archive storage class is referred to as GLACIER. You can convert the storage
// class of OSS objects based on your requirements.
// ref: https://www.alibabacloud.com/help/en/oss/developer-reference/compatibility-with-amazon-s3
// and https://www.alibabacloud.com/help/en/oss/user-guide/overview-53/
pub const OSS_STORAGE_CLASS_DEFAULT: &str = OSS_STORAGE_CLASS_STANDARD;
pub const OSS_STORAGE_CLASS_STANDARD: &str = "Standard";
pub const OSS_STORAGE_CLASS_IA: &str = "IA";
pub const OSS_STORAGE_CLASS_ARCHIVE: &str = "Archive";

const AWS_DOMAIN_STRING: &str = "amazonaws.com";

const SMALL_FILE_THRESHOLD_BYTES: u64 = 1024 * 1024; // 1MB

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageClass {
    Standard,
    InfrequentAccess,
    GlacierInstantRetrieval,
    IntelligentTiering,
    Default,
}

impl StorageClass {
    fn from_str(s: &str) -> Self {
        match s.to_ascii_uppercase().as_str() {
            "STANDARD" => Self::Standard,
            "STANDARD_IA" | "IA" => Self::InfrequentAccess,
            "GLACIER_IR" | "ARCHIVE" => Self::GlacierInstantRetrieval,
            "INTELLIGENT_TIERING" => Self::IntelligentTiering,
            _ => Self::Default,
        }
    }
}

/// Supported S3-compatible or cloud object storage providers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CloudProvider {
    Aws,
    Aliyun,
    Unknown,
}

impl CloudProvider {
    /// Try to infer the cloud provider based on the endpoint.
    pub fn from_endpoint(endpoint: &str) -> Self {
        let lower = endpoint.to_ascii_lowercase();
        if lower.contains(AWS_DOMAIN_STRING) {
            CloudProvider::Aws
        } else if lower.contains(aliyun::DOMAIN_STRING) {
            CloudProvider::Aliyun
        } else {
            CloudProvider::Unknown
        }
    }

    /// Returns the provider-specific header prefix.
    pub fn header_prefix(&self) -> &'static str {
        match self {
            CloudProvider::Aws => "x-amz-",
            CloudProvider::Aliyun => "x-oss-",
            _ => "x-amz-",
        }
    }

    /// Builds a full header key name given a suffix.
    pub fn build_header_key(&self, key: &str) -> String {
        format!("{}{}", self.header_prefix(), key)
    }

    /// Set the corresponding cloud-specific headers.
    pub fn add_provider_header(&self, req: &mut SignedRequest, key: &str, value: &str) {
        let value = if key.ends_with("storage-class") {
            self.storage_class_str(StorageClass::from_str(value))
        } else {
            value
        };
        let key_suffix = key
            .strip_prefix(CloudProvider::Aws.header_prefix())
            .unwrap_or(key);
        req.add_header(self.build_header_key(key_suffix), value);
    }

    /// Convert to the provider-specific storage class string.
    /// Alibaba cloud only support "Standard", "IA" and "Archive",
    /// Unknown storage class will be treated as "Standard".
    /// ref: https://www.alibabacloud.com/help/en/oss/developer-reference/compatibility-with-amazon-s3
    pub fn storage_class_str(&self, storage_class: StorageClass) -> &'static str {
        match self {
            CloudProvider::Aliyun => match storage_class {
                StorageClass::Standard | StorageClass::IntelligentTiering => {
                    OSS_STORAGE_CLASS_STANDARD
                }
                StorageClass::InfrequentAccess => OSS_STORAGE_CLASS_IA,
                StorageClass::GlacierInstantRetrieval => OSS_STORAGE_CLASS_ARCHIVE,
                StorageClass::Default => OSS_STORAGE_CLASS_DEFAULT,
            },
            _ => match storage_class {
                StorageClass::Standard => STORAGE_CLASS_STANDARD,
                StorageClass::InfrequentAccess => STORAGE_CLASS_STANDARD_IA,
                StorageClass::GlacierInstantRetrieval => STORAGE_CLASS_GLACIER_IR,
                StorageClass::IntelligentTiering => STORAGE_CLASS_INTELLIGENT_TIERING,
                StorageClass::Default => STORAGE_CLASS_DEFAULT,
            },
        }
    }
}

#[derive(Clone)]
pub struct S3Fs {
    core: Arc<S3FsCore>,
}

impl S3Fs {
    pub fn new(
        prefix: String,
        endpoint: String,
        key_id: String,
        secret_key: String,
        region: String,
        bucket: String,
        options: ConnOptions,
        read_only: bool,
    ) -> Self {
        let core = Arc::new(S3FsCore::new(
            endpoint, key_id, secret_key, region, bucket, prefix, options, read_only,
        ));
        Self { core }
    }

    pub fn new_from_config(conf: Config) -> Self {
        Self::new(
            conf.prefix,
            conf.s3_endpoint,
            conf.s3_key_id,
            conf.s3_secret_key,
            conf.s3_region,
            conf.s3_bucket,
            conf.conn_options,
            conf.read_only,
        )
    }

    #[cfg(any(test, feature = "testexport"))]
    pub fn new_for_test(s3c: rusoto_core::Client, bucket: String, prefix: String) -> Self {
        Self {
            core: Arc::new(S3FsCore::new_with_s3_client(
                s3c,
                "".to_string(),
                "local".to_string(),
                bucket,
                prefix,
                ConnOptions::default(),
                false,
            )),
        }
    }
}

impl Deref for S3Fs {
    type Target = S3FsCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

pub struct S3FsCore {
    s3c: rusoto_core::Client,
    hostname: String,
    region: Region,
    bucket: String,
    prefix: String,
    runtime: Option<tokio::runtime::Runtime>,
    provider: CloudProvider,
    virtual_host: bool,
    opts: ConnOptions,
    read_only: bool, // For safety when used for recovery.
    verifier: OperationVerifier,
}

impl S3FsCore {
    pub fn new(
        endpoint: String,
        key_id: String,
        secret_key: String,
        region: String,
        bucket: String,
        prefix: String,
        options: ConnOptions,
        read_only: bool,
    ) -> Self {
        let mut config = rusoto_core::HttpConfig::new();
        config.read_buf_size(256 * 1024);
        config.pool_idle_timeout(Some(options.pool_idle_timeout.0));
        let use_tls = endpoint.starts_with("https");
        let default_provider = aws::CredentialsProvider::new().unwrap();
        let static_provider =
            rusoto_credential::StaticProvider::new(key_id.clone(), secret_key, None, None);
        let mut http_connector = hyper::client::connect::HttpConnector::new();
        http_connector.set_connect_timeout(Some(options.conn_timeout.0));
        http_connector.set_keepalive(Some(options.keep_alive_duration.0));
        let s3c = if use_tls {
            let https_connector = HttpsConnector::new_with_connector(http_connector);
            let http_client = HttpClient::from_connector_with_config(https_connector, config);
            if key_id.is_empty() {
                if endpoint.contains(aliyun::DOMAIN_STRING) {
                    let ali_provider = aliyun::new_credential_provider().unwrap();
                    rusoto_core::Client::new_with(ali_provider, http_client)
                } else {
                    rusoto_core::Client::new_with(default_provider, http_client)
                }
            } else {
                rusoto_core::Client::new_with(static_provider, http_client)
            }
        } else {
            let http_client = HttpClient::from_connector_with_config(http_connector, config);
            if key_id.is_empty() {
                if endpoint.contains(aliyun::DOMAIN_STRING) {
                    let ali_provider = aliyun::new_credential_provider().unwrap();
                    rusoto_core::Client::new_with(ali_provider, http_client)
                } else {
                    rusoto_core::Client::new_with(default_provider, http_client)
                }
            } else {
                rusoto_core::Client::new_with(static_provider, http_client)
            }
        };
        let endpoint = if endpoint.is_empty() {
            format!("http://s3.{}.amazonaws.com", region.as_str())
        } else {
            endpoint
        };
        Self::new_with_s3_client(s3c, endpoint, region, bucket, prefix, options, read_only)
    }

    pub fn new_with_s3_client(
        s3c: rusoto_core::Client,
        endpoint: String,
        region: String,
        bucket: String,
        mut prefix: String,
        options: ConnOptions,
        read_only: bool,
    ) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("s3")
            .build()
            .unwrap();
        if prefix.is_empty() {
            prefix.push_str("default")
        }
        let no_schema_endpoint = endpoint
            .find("://")
            .map(|p| &endpoint[p + 3..])
            .unwrap_or(&endpoint);
        let provider = CloudProvider::from_endpoint(&endpoint);
        // local deployed s3 service like minio does not support virtual host
        // addressing, it always has a port defined at the end.
        let virtual_host = !no_schema_endpoint.contains(':');
        let hostname = if virtual_host {
            format!("{}.{}", &bucket, no_schema_endpoint)
        } else {
            no_schema_endpoint.to_string()
        };
        let region = Region::Custom {
            name: region,
            endpoint,
        };
        Self {
            s3c,
            hostname,
            region,
            bucket,
            prefix,
            runtime: Some(runtime),
            provider,
            virtual_host,
            opts: options,
            read_only,
            verifier: OperationVerifier::new(1000),
        }
    }

    fn is_on_aws(&self) -> bool {
        self.provider == CloudProvider::Aws
    }

    fn is_on_aliyun(&self) -> bool {
        self.provider == CloudProvider::Aliyun
    }

    pub fn storage_class_str(&self, storage_class: StorageClass) -> &'static str {
        self.provider.storage_class_str(storage_class)
    }

    fn is_err_retryable<T>(&self, rustoto_err: &RusotoError<T>) -> bool {
        match rustoto_err {
            RusotoError::Service(_) => true,
            RusotoError::HttpDispatch(_) => true,
            RusotoError::InvalidDnsName(_) => false,
            RusotoError::Credentials(cred) => cred.message.contains("Timeout"),
            RusotoError::Validation(_) => false,
            RusotoError::ParseError(_) => false,
            RusotoError::Unknown(resp) => resp.status.is_server_error(),
            RusotoError::Blocking => false,
        }
    }

    fn is_err_not_found<T>(&self, rustoto_err: &RusotoError<T>) -> bool {
        match rustoto_err {
            RusotoError::Unknown(resp) => resp.status == StatusCode::NOT_FOUND,
            _ => false,
        }
    }

    async fn sleep_for_retry(&self, retry_cnt: &mut u32, file_name: &str) -> bool {
        if *retry_cnt < self.opts.max_retry_count {
            *retry_cnt += 1;
            let retry_sleep = 2u32.pow(*retry_cnt) * self.opts.retry_sleep_interval.0;
            tokio::time::sleep(retry_sleep).await;
            true
        } else {
            error!(
                "read file {}, reach max retry count {}",
                file_name, self.opts.max_retry_count
            );
            false
        }
    }

    /// list gets a list of file ids(full path) greater than `start_after`, with
    /// optional prefix `prefix`.
    ///
    /// The result contains:
    ///     A vector of file content with `key` & `last_modified` timestamp.
    ///     A boolean `has_more` indicate if there is more.
    ///     An optional `next_start_after` for next loop if `has_more` is true.
    ///
    /// Note:
    ///     `prefix` should NOT be contained in `start_after`.
    ///     The file ids in result are in full path, including `S3Fs.prefix` and
    /// `prefix`.
    ///
    /// Ref: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
    pub async fn list(
        &self,
        start_after: &str,
        prefix: Option<&str>,
        max_keys: Option<u32>,
    ) -> crate::dfs::Result<(
        Vec<ListObjectContent>,
        bool,           // has_more. Deprecated, use `next_start_after`
        Option<String>, // next_start_after
    )> {
        let prefix = format!("{}/{}", self.prefix.clone(), prefix.unwrap_or_default());
        let start_after = format!("{}{}", prefix, start_after);
        let mut retry_cnt = 0;
        loop {
            let mut req = self.new_request("GET", "");
            let mut params = Params::new();
            params.put("list-type", "2");
            params.put("start-after", &start_after);
            params.put("prefix", &prefix);
            if let Some(max_keys) = max_keys {
                params.put("max-keys", max_keys.to_string());
            }
            req.set_params(params);
            let mut result = self.dispatch(req, ListObjectsV2Error::from_response).await;
            if result.is_ok() {
                let mut response = result.unwrap();
                let body_res = self.read_body(&mut response).await;
                if body_res.is_ok() {
                    let body = body_res.unwrap();
                    let body_str = body.to_str().unwrap();
                    let list: ListObjects = quick_xml::de::from_str(body_str).unwrap();
                    let next_start_after = list.is_truncated.then(|| {
                        list.contents.last().unwrap().key.as_str()[prefix.len()..].to_string()
                    });
                    return Ok((list.contents, list.is_truncated, next_start_after));
                } else {
                    result = Err(body_res.unwrap_err().into());
                }
            }
            let err = result.unwrap_err();
            if self.is_err_not_found(&err) {
                return Ok((vec![], false, None));
            } else if self.is_err_retryable(&err) && retry_cnt < self.opts.max_retry_count {
                retry_cnt += 1;
                let retry_sleep = 2u32.pow(retry_cnt) * self.opts.retry_sleep_interval.0;
                tokio::time::sleep(retry_sleep).await;
                continue;
            }
            error!(
                "failed to list files start after {}, reach max retry count {}, err {:?}",
                start_after, self.opts.max_retry_count, err,
            );
            return Err(err.into());
        }
    }

    pub async fn list_folders(
        &self,
        prefix: &str,
        delimiter: Option<&str>,
    ) -> crate::dfs::Result<Vec<String>> {
        let prefix = format!("{}/{}", self.prefix.clone(), prefix);
        let delimiter = delimiter.unwrap_or("/");
        let mut retry_cnt = 0;
        loop {
            let mut req = self.new_request("GET", "");
            let mut params = Params::new();
            params.put("list-type", "2");
            params.put("start-after", &prefix);
            params.put("prefix", &prefix);
            params.put("delimiter", delimiter);
            req.set_params(params);
            let mut result = self.dispatch(req, ListObjectsV2Error::from_response).await;
            if result.is_ok() {
                let mut response = result.unwrap();
                let body_res = self.read_body(&mut response).await;
                if body_res.is_ok() {
                    let body = body_res.unwrap();
                    let body_str = body.to_str().unwrap();
                    let list: ListObjects = quick_xml::de::from_str(body_str).unwrap();
                    let prefixes = list
                        .common_prefixes
                        .into_iter()
                        .map(|p| p.prefix)
                        .collect::<Vec<_>>();
                    return Ok(prefixes);
                } else {
                    result = Err(body_res.unwrap_err().into());
                }
            }
            let err = result.unwrap_err();
            if self.is_err_not_found(&err) {
                return Ok(vec![]);
            } else if self.is_err_retryable(&err) && retry_cnt < self.opts.max_retry_count {
                retry_cnt += 1;
                let retry_sleep = 2u32.pow(retry_cnt) * self.opts.retry_sleep_interval.0;
                tokio::time::sleep(retry_sleep).await;
                continue;
            }
            error!(
                "failed to list folders prefix {}, reach max retry count {}, err {:?}",
                prefix, self.opts.max_retry_count, err,
            );
            return Err(err.into());
        }
    }

    pub async fn is_removed(&self, file_key: &str) -> Result<bool, dfs::Error> {
        let mut retry_cnt = 0;
        loop {
            let req = self.new_tagging_request("GET", file_key);
            let mut result = self
                .dispatch(req, GetObjectTaggingError::from_response)
                .await;
            if result.is_ok() {
                let mut resp = result.unwrap();
                let body_res = self.read_body(&mut resp).await;
                if body_res.is_ok() {
                    let body = body_res.unwrap();
                    let body_str = body.to_str().unwrap();
                    let tagging: Tagging = quick_xml::de::from_str(body_str).unwrap();
                    return Ok(tagging.has_deleted_tag());
                } else {
                    result = Err(body_res.unwrap_err().into());
                }
            }
            let err = result.unwrap_err();
            if let RusotoError::Service(_) = err {
                return Err(dfs::Error::S3(format!(
                    "Get file {} tagging fail {:?}",
                    file_key, err
                )));
            }
            if self.is_err_retryable(&err) && retry_cnt < self.opts.max_retry_count {
                retry_cnt += 1;
                let retry_sleep = 2u32.pow(retry_cnt) * self.opts.retry_sleep_interval.0;
                warn!(
                    "Get file {} tagging fail {:?}, retry_cnt {}, retry after {:?}",
                    file_key, err, retry_cnt, retry_sleep
                );
                tokio::time::sleep(retry_sleep).await;
                continue;
            }
            return Err(dfs::Error::S3(format!(
                "Get file {} tagging, reach max retry count {}, err {:?}",
                file_key, self.opts.max_retry_count, err,
            )));
        }
    }

    pub async fn exist(&self, key: String, file_name: String) -> Result<bool, dfs::Error> {
        match self.head_object(key, file_name).await {
            Ok(_) => Ok(true),
            Err(dfs::Error::NoSuchKey(_)) => Ok(false),
            Err(err) => Err(err),
        }
    }

    pub async fn object_size(&self, key: String, file_name: String) -> Result<u64, dfs::Error> {
        match self.head_object(key.clone(), file_name).await {
            Ok(headers) => {
                if let Some(content_length) = headers.get("Content-Length") {
                    if let Ok(length) = content_length.parse::<u64>() {
                        return Ok(length);
                    }
                    Err(dfs::Error::Other(format!(
                        "failed to parse {} Content-Length {}",
                        key, content_length
                    )))
                } else {
                    Err(dfs::Error::Other(format!(
                        "failed to get Content-Length from {}",
                        key
                    )))
                }
            }
            Err(err) => Err(err),
        }
    }

    pub async fn head_object(
        &self,
        key: String,
        file_name: String,
    ) -> Result<http::HeaderMap<String>, dfs::Error> {
        let mut retry_cnt = 0;
        let start_time = Instant::now_coarse();
        loop {
            let req = self.new_request("HEAD", &key);
            let result = self.dispatch(req, HeadObjectError::from_response).await;
            if result.is_ok() {
                let resp = result.unwrap();
                info!(
                    "head object {}, takes {:?}, retry {}",
                    &file_name,
                    start_time.saturating_elapsed(),
                    retry_cnt
                );
                return Ok(resp.headers.clone());
            }
            let err = result.unwrap_err();
            if let RusotoError::Service(HeadObjectError::NoSuchKey(err_msg)) = &err {
                warn!("file {} not exist, err msg {}", &file_name, err_msg);
                return Err(dfs::Error::NoSuchKey(err_msg.to_string()));
            }
            if self.is_err_not_found(&err) {
                warn!("file {} not exist, err {}", &file_name, err.to_string());
                return Err(dfs::Error::NoSuchKey(err.to_string()));
            }
            if self.is_err_retryable(&err) && self.sleep_for_retry(&mut retry_cnt, &file_name).await
            {
                KVENGINE_DFS_RETRY_COUNTER_VEC
                    .with_label_values(&["head"])
                    .inc();
                warn!("retry head file {}, error {:?}", &file_name, &err);
                continue;
            }
            return Err(dfs::Error::S3(err.to_string()));
        }
    }

    fn new_request(&self, method: &str, key: &str) -> SignedRequest {
        let path = if self.virtual_host {
            format!("/{}", key)
        } else {
            format!("/{}/{}", &self.bucket, key)
        };
        let mut req = SignedRequest::new(method, "s3", &self.region, &path);
        req.scheme = Some("http".to_string());
        req
    }

    fn new_tagging_request(&self, method: &str, key: &str) -> SignedRequest {
        let mut req = self.new_request(method, key);
        let mut params = Params::new();
        params.put_key("tagging");
        req.set_params(params);
        req
    }

    async fn dispatch<E>(
        &self,
        req: SignedRequest,
        from_response: fn(BufferedHttpResponse) -> RusotoError<E>,
    ) -> Result<Response, RusotoError<E>> {
        self.dispatch_with_timeout(req, self.opts.dispatch_timeout.0, from_response)
            .await
    }

    async fn dispatch_with_timeout<E>(
        &self,
        mut req: SignedRequest,
        timeout: Duration,
        from_response: fn(BufferedHttpResponse) -> RusotoError<E>,
    ) -> Result<Response, RusotoError<E>> {
        req.set_hostname(Some(self.hostname.clone()));
        let mut resp = self.s3c.sign_and_dispatch_timeout(req, timeout).await?;
        if !resp.status.is_success() {
            let buffered = resp.buffer().await.map_err(RusotoError::HttpDispatch)?;
            return Err(from_response(buffered));
        }
        Ok(Response { resp })
    }

    async fn read_body(&self, resp: &mut Response) -> Result<Bytes, HttpDispatchError> {
        let (writer, _) = self
            .read_body_to_writer(resp, &|| Ok(BytesMut::new().writer()))
            .await?;
        Ok(writer.into_inner().freeze())
    }

    async fn read_body_to_writer<W, F>(
        &self,
        resp: &mut Response,
        build_writer: &F,
    ) -> Result<(W, u64 /* read_len */), HttpDispatchError>
    where
        W: ReservableWriter + Send + 'static,
        F: Fn() -> std::io::Result<W> + 'static,
    {
        let mut writer =
            build_writer().map_err(|e| HttpDispatchError::new(format!("build_writer: {:?}", e)))?;
        // Some interfaces may not return Content-Length header, e.g. ListObjects.
        let cap = resp
            .headers
            .get("Content-Length")
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or_default();
        debug!("headers: {:?}, cap: {}", resp.headers, cap);
        if cap > 0 {
            writer.reserve_capacity(cap);
        }
        let mut read_len = 0;
        while let Some(res) = tokio::time::timeout(self.opts.read_body_timeout.0, resp.body.next())
            .await
            .map_err(|e| HttpDispatchError::new(format!("read body timeout {:?}", e)))?
        {
            let chunk = res.map_err(|e| HttpDispatchError::new(format!("{:?}", e)))?;
            read_len += chunk.len() as u64;
            writer
                .write_all(&chunk)
                .map_err(|e| HttpDispatchError::new(format!("write_all: {:?}", e)))?;
        }
        if cap > 0 && read_len != cap {
            warn!("content length mismatch";
                "content_length" => cap, "read_len" => read_len);
            debug_assert!(false);
        }
        Ok((writer, read_len))
    }

    pub async fn get_object(
        &self,
        key: String,
        file_name: String,
        opts: GetObjectOptions,
    ) -> crate::dfs::Result<Bytes> {
        let (writer, _) = self
            .get_object_to_writer(key, file_name, opts, &|| Ok(BytesMut::new().writer()))
            .await?;
        Ok(writer.into_inner().freeze())
    }

    pub async fn get_object_with_cache(
        &self,
        key: String,
        file_name: String,
        opts: GetObjectOptions,
        cache_with_hook: Option<&ObjectCacheWithHook>,
    ) -> crate::dfs::Result<Bytes> {
        match cache_with_hook {
            Some(cache_with_hook) => {
                let ObjectCacheWithHook { cache, hook } = cache_with_hook;
                let with = async {
                    match self.get_object(key.clone(), file_name, opts).await {
                        Ok(data) => hook.invoke(data).map_err(|e| Error::Hook(e)),
                        Err(err) => Err(err),
                    }
                };
                cache.get_or_insert_async(&key, with).await
            }
            None => self.get_object(key, file_name, opts).await,
        }
    }

    pub async fn get_object_to_writer<W, F>(
        &self,
        key: String,
        file_name: String,
        opts: GetObjectOptions,
        build_writer: F,
    ) -> crate::dfs::Result<(W, u64)>
    where
        W: ReservableWriter + Send + 'static,
        F: Fn() -> std::io::Result<W> + 'static,
    {
        let mut retry_cnt = 0;
        let start_time = Instant::now_coarse();
        loop {
            let mut req = self.new_request("GET", &key);
            if !opts.is_full_range() {
                req.add_header("Range", &format!("bytes={}", opts.range_string()));
            }
            let mut result = self.dispatch(req, GetObjectError::from_response).await;

            if result.is_ok() {
                let mut resp = result.unwrap();
                let res = self.read_body_to_writer(&mut resp, &build_writer).await;
                match res {
                    Ok((writer, object_len)) => {
                        info!(
                            "read file {}, size {}, takes {:?}, retry {}",
                            &file_name,
                            object_len,
                            start_time.saturating_elapsed(),
                            retry_cnt
                        );

                        KVENGINE_DFS_THROUGHPUT_VEC
                            .with_label_values(&["read"])
                            .inc_by(object_len);
                        KVENGINE_DFS_LATENCY_VEC
                            .with_label_values(&["read"])
                            .observe(start_time.saturating_elapsed().as_millis() as f64);
                        return Ok((writer, object_len));
                    }
                    Err(err) => result = Err(err.into()),
                }
            }
            let err = result.unwrap_err();
            if let RusotoError::Service(GetObjectError::NoSuchKey(err_msg)) = &err {
                error!("file {} not exist, err msg {}", &file_name, err_msg);
                return Err(dfs::Error::NoSuchKey(format!(
                    "file {} not exist,",
                    &file_name
                )));
            }
            if self.is_err_not_found(&err) {
                error!("file {} not exist, err {}", &file_name, err.to_string());
                return Err(dfs::Error::NoSuchKey(format!(
                    "file {} not exist,",
                    &file_name
                )));
            }
            if self.is_err_retryable(&err) && self.sleep_for_retry(&mut retry_cnt, &file_name).await
            {
                KVENGINE_DFS_RETRY_COUNTER_VEC
                    .with_label_values(&["read"])
                    .inc();
                warn!("retry read file {}, error {:?}", &file_name, &err);
                continue;
            }
            error!("read file {}, error {:?}", &file_name, err);
            return Err(err.into());
        }
    }

    pub async fn put_object(
        &self,
        key: String,
        data: Bytes,
        file_name: String,
    ) -> crate::dfs::Result<()> {
        self.put_object_with_options(key, data, file_name, None, None, None)
            .await
    }

    async fn put_object_with_options(
        &self,
        key: String,
        data: Bytes,
        file_name: String,
        tagging: Option<&Tagging>,
        storage_class: Option<&str>,
        checksum: Option<u32>, // checksum saved in big-endian order
    ) -> crate::dfs::Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        fail_point!(
            "s3fs_put_wal_chunks_error",
            key.contains("wal_chunks"),
            |_| Err(Error::Other(
                "s3fs_put_wal_chunks_error failpoint".to_string(),
            ))
        );

        let mut retry_cnt = 0;
        let start_time = Instant::now();
        let data_len = data.len();
        loop {
            let mut req = self.new_request("PUT", &key);
            req.add_header("Content-Length", &format!("{}", data.len()));
            if let Some(tagging) = tagging {
                req.add_header("x-amz-tagging", &tagging.to_url_encoded());
            }
            if let Some(storage_class) = storage_class.as_ref() {
                if self.is_on_aws() || self.is_on_aliyun() {
                    self.provider.add_provider_header(
                        &mut req,
                        "x-amz-storage-class",
                        storage_class,
                    );
                } else {
                    debug!(
                        "{} ignore storage_class {} which is not supported by {}",
                        key, storage_class, self.hostname
                    );
                }
            }
            if let Some(checksum) = checksum {
                if self.is_on_aws() {
                    // `x-amz-checksum-crc32c` value is base64 encoded in big-endian order.
                    let checksum = base64::encode(checksum.to_be_bytes());
                    req.add_header("x-amz-checksum-crc32", &checksum);
                } else {
                    debug!(
                        "{} ignore x-amz-checksum-crc32c which is not supported by {}",
                        key, self.hostname
                    );
                }
            }
            let data = data.clone();
            let stream = futures::stream::once(async move { Ok(data) });
            req.set_payload_stream(rusoto_core::ByteStream::new(stream));
            let data_mb = data_len as u32 / (1024 * 1024);
            let timeout = self.opts.dispatch_timeout.0 + self.opts.write_timeout_per_mb.0 * data_mb;
            let result = self
                .dispatch_with_timeout(req, timeout, PutObjectError::from_response)
                .await;
            if result.is_ok() {
                info!(
                    "create file {}, size {}, takes {:?}, retry {}",
                    &file_name,
                    data_len,
                    start_time.saturating_elapsed(),
                    retry_cnt
                );
                KVENGINE_DFS_THROUGHPUT_VEC
                    .with_label_values(&["write"])
                    .inc_by(data_len as u64);
                KVENGINE_DFS_LATENCY_VEC
                    .with_label_values(&["write"])
                    .observe(start_time.saturating_elapsed().as_millis() as f64);
                return Ok(());
            }
            let err = result.unwrap_err();
            if self.is_err_retryable(&err) {
                if retry_cnt < self.opts.max_retry_count {
                    KVENGINE_DFS_RETRY_COUNTER_VEC
                        .with_label_values(&["write"])
                        .inc();
                    retry_cnt += 1;
                    let retry_sleep = 2u32.pow(retry_cnt) * self.opts.retry_sleep_interval.0;
                    tokio::time::sleep(retry_sleep).await;
                    warn!("retry create file {}, error {:?}", &file_name, &err);
                    continue;
                } else {
                    error!(
                        "create file {}, takes {:?}, reach max retry count {}",
                        &file_name,
                        start_time.saturating_elapsed(),
                        self.opts.max_retry_count
                    );
                }
            }
            return Err(err.into());
        }
    }

    // Ref: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html
    pub async fn delete_object(&self, key: String, file_name: String) -> crate::dfs::Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        let mut retry_cnt = 0;
        let start_time = Instant::now();
        loop {
            let req = self.new_request("DELETE", &key);
            let result = self.dispatch(req, DeleteObjectError::from_response).await;
            if result.is_ok() {
                info!(
                    "delete file {}, takes {:?}, retry {}",
                    &file_name,
                    start_time.saturating_elapsed(),
                    retry_cnt
                );
                KVENGINE_DFS_LATENCY_VEC
                    .with_label_values(&["delete"])
                    .observe(start_time.saturating_elapsed().as_millis() as f64);
                return Ok(());
            }
            let err = result.unwrap_err();
            if self.is_err_retryable(&err) {
                if retry_cnt < self.opts.max_retry_count {
                    KVENGINE_DFS_RETRY_COUNTER_VEC
                        .with_label_values(&["delete"])
                        .inc();
                    retry_cnt += 1;
                    let retry_sleep = 2u32.pow(retry_cnt) * self.opts.retry_sleep_interval.0;
                    tokio::time::sleep(retry_sleep).await;
                    warn!(
                        "retry delete file {}, error {:?}, retry cnt {}, retry after {:?}",
                        &file_name, &err, retry_cnt, retry_sleep
                    );
                    continue;
                } else {
                    error!(
                        "delete file {}, takes {:?}, reach max retry count {}",
                        &file_name,
                        start_time.saturating_elapsed(),
                        self.opts.max_retry_count
                    );
                }
            }
            return Err(err.into());
        }
    }

    /// Copy object from `source_file_id` to `target_file_id`.
    ///
    /// `source_file_id` and `target_file_id` can be the same. And it's the only
    /// way to update the object creation timestamp, which is used for
    /// expiration rules of lifecycle.
    ///
    /// `target_tags`: replace tags if some, otherwise copy from source.
    ///
    /// `target_storage_class`: copy to another storage class if some. Note than
    /// only AWS S3 support storage class.
    async fn copy_object(
        &self,
        source_key: &str,
        target_key: &str,
        target_tagging: Option<&Tagging>,
        target_storage_class: Option<&str>,
    ) -> Result<(), dfs::Error> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        let mut retry_cnt = 0;
        let start_time = Instant::now_coarse();
        let full_source_key = format!("/{}/{}", self.bucket, source_key);
        loop {
            let mut req = self.new_request("PUT", target_key);
            // `x-oss-copy-source` not effective on Aliyun, but `x-amz-copy-source` works.
            req.add_header("x-amz-copy-source", &full_source_key);
            req.add_header("x-amz-metadata-directive", "REPLACE");
            if let Some(target_tagging) = target_tagging {
                req.add_header("x-amz-tagging", &target_tagging.to_url_encoded());
                req.add_header("x-amz-tagging-directive", "REPLACE");
            }
            if let Some(target_storage_class) = target_storage_class.as_ref() {
                if self.is_on_aws() || self.is_on_aliyun() {
                    self.provider.add_provider_header(
                        &mut req,
                        "x-amz-storage-class",
                        target_storage_class,
                    );
                } else {
                    debug!(
                        "{} ignore target_storage_class {} which is not supported by {}",
                        target_key, target_storage_class, self.hostname
                    );
                }
            }
            if let Err(err) = self.dispatch(req, CopyObjectError::from_response).await {
                if retry_cnt < self.opts.max_retry_count {
                    retry_cnt += 1;
                    let retry_sleep = 2u32.pow(retry_cnt) * self.opts.retry_sleep_interval.0;
                    warn!(
                        "retry copy file {}, retry count {}, retry after {:?}, err {:?}",
                        target_key, retry_cnt, retry_sleep, err,
                    );
                    tokio::time::sleep(retry_sleep).await;
                    continue;
                } else {
                    let err_msg = format!(
                        "failed to copy file {} from {}, reach max retry count {}, err {:?}",
                        target_key, source_key, self.opts.max_retry_count, err,
                    );
                    error!("{}", err_msg);
                    return Err(dfs::Error::S3(err_msg));
                }
            }
            info!(
                "copy object {} from {}, takes {:?}, retry {}",
                target_key,
                source_key,
                start_time.saturating_elapsed(),
                retry_cnt
            );
            if self.verifier.should_verify() {
                let size = self
                    .object_size(target_key.to_string(), target_key.to_string())
                    .await
                    .unwrap_or_default();
                if size == 0 {
                    return Err(dfs::Error::S3(format!(
                        "failed to copy object from {} to {}",
                        source_key, target_key
                    )));
                }
            }
            return Ok(());
        }
    }

    pub async fn retain_file(&self, file_key: &str) -> Result<(), dfs::Error> {
        if self.is_removed(file_key).await? {
            let empty_tagging = Tagging::default();
            self.copy_object(
                file_key,
                file_key,
                Some(&empty_tagging),
                Some(self.storage_class_str(StorageClass::Default)),
            )
            .await?;
        }
        Ok(())
    }

    /// Choose proper storage class for removed files.
    fn choose_storage_class_for_removed_files(&self, file_len: Option<u64>) -> &'static str {
        // STORAGE_CLASS_STANDARD_IA is more cost efficient than STORAGE_CLASS_STANDARD
        // for NOT small files.
        if file_len.is_some() && file_len.unwrap() > SMALL_FILE_THRESHOLD_BYTES {
            self.storage_class_str(StorageClass::InfrequentAccess)
        } else {
            self.storage_class_str(StorageClass::Default)
        }
    }
}

impl Drop for S3FsCore {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            std::thread::spawn(|| drop(runtime));
        }
    }
}

impl ObjectStorage for S3Fs {
    fn put_objects(&self, objects: Vec<(String, Bytes)>) -> Result<(), String> {
        let put_object = |key: String, data: Bytes| {
            let full_key = format!("{}/{}", self.prefix, key);
            let fs = self.clone();
            let checksum = if self.is_on_aws() {
                Some(crc32fast::hash(&data))
            } else {
                None
            };
            let storage_class_default = self.storage_class_str(StorageClass::Default);
            async move {
                fs.put_object_with_options(
                    full_key,
                    data,
                    key.clone(),
                    None,
                    Some(storage_class_default),
                    checksum,
                )
                .await
                .map_err(|err| format!("put {} failed {:?}", &key, err))
            }
        };

        let runtime = self.get_runtime();
        let len = objects.len();

        if len == 1 {
            let (key, data) = objects.into_iter().next().unwrap();
            return runtime.block_on(put_object(key, data));
        }

        let mut handles = Vec::with_capacity(len);
        for (key, data) in objects {
            handles.push(runtime.spawn(put_object(key, data)));
        }

        let errs: Vec<_> = runtime
            .block_on(futures::future::join_all(handles))
            .into_iter()
            .filter_map(|r| r.unwrap().err())
            .collect();
        if !errs.is_empty() {
            return Err(format!("{:?}", errs));
        }
        Ok(())
    }

    fn get_objects(
        &self,
        keys: Vec<(String, GetObjectOptions)>,
        cache: Option<&ObjectCacheWithHook>,
    ) -> Result<Vec<(String, Bytes)>, String> {
        let get_object =
            |key: String, opts: GetObjectOptions, cache: Option<&ObjectCacheWithHook>| {
                let full_key = format!("{}/{}", self.prefix, key);
                let fs = self.clone();
                let cache = cache.cloned();
                async move {
                    match fs
                        .get_object_with_cache(full_key, key.clone(), opts, cache.as_ref())
                        .await
                    {
                        Ok(data) => Ok((key, data)),
                        Err(err) => Err(format!("get {} failed {:?}", &key, err)),
                    }
                }
            };

        let runtime = self.get_runtime();
        if keys.len() == 1 {
            let mut keys = keys;
            let (key, opts) = keys.pop().unwrap();
            let res = runtime.block_on(get_object(key, opts, cache))?;
            return Ok(vec![res]);
        }

        let mut join_set = tokio::task::JoinSet::new();
        for (key, opts) in keys {
            join_set.spawn_on(get_object(key, opts, cache), runtime.handle());
        }

        runtime.block_on(async move {
            let mut objects: Vec<(String, Bytes)> = vec![];
            while let Some(res) = join_set.join_next().await {
                let res = box_join_err!(res).map_err(|e: BoxError| e.to_string())??;
                objects.push(res);
            }
            Ok(objects)
        })
    }

    fn list_objects(
        &self,
        start_after: &str,
        prefix: Option<&str>,
        max_keys: Option<u32>,
    ) -> Result<(Vec<ListObjectContent>, Option<String>), String> {
        let runtime = self.get_runtime();
        runtime
            .block_on(self.list(start_after, prefix, max_keys))
            .map(|v| (v.0, v.2))
            .map_err(|err| format!("list failed {:?}", err))
    }
}

#[async_trait]
impl Dfs for S3Fs {
    async fn read_file(&self, file_id: u64, opts: Options) -> crate::dfs::Result<Bytes> {
        let filename = if let Some(end_off) = opts.end_off {
            format!("{}-{}-{}.seg", file_id, opts.start_off, end_off)
        } else {
            format!("{}.{}", file_id, opts.file_type.suffix())
        };
        self.get_object(
            self.file_key(file_id, opts.file_type),
            filename,
            GetObjectOptions {
                start_off: Some(opts.start_off),
                end_off: opts.end_off,
            },
        )
        .await
    }

    async fn create(&self, file_id: u64, data: Bytes, opts: Options) -> crate::dfs::Result<()> {
        // Calculate checksum for the file. This can be used to ensure the data
        // integrity when saving to dfs (s3 only) and verify the data integrity when
        // getting objects from dfs.
        let checksum = if self.is_on_aws() {
            Some(crc32fast::hash(&data))
        } else {
            None
        };
        self.put_object_with_options(
            self.file_key(file_id, opts.file_type),
            data,
            format!("{}.{}", file_id, opts.file_type.suffix()),
            None,
            Some(self.storage_class_str(StorageClass::Default)),
            checksum,
        )
        .await
    }

    /// Logically remove the file on S3 by tagging with "deleted=true".
    /// And the file would be permanently removed after `gc_lifetime`. See
    /// `DfsGc`.
    async fn remove(&self, file_id: u64, file_len: Option<u64>, opts: Options) {
        // Only AWS supports storage class.
        let target_storage_class = if self.is_on_aws() || self.is_on_aliyun() {
            let new_storage_class = self.choose_storage_class_for_removed_files(file_len);
            (new_storage_class != self.storage_class_str(StorageClass::Default))
                .then_some(new_storage_class)
        } else {
            None
        };
        let target_tagging = Tagging::new_single_deleted();
        let file_key = self.file_key(file_id, opts.file_type);
        let _ = self
            .copy_object(
                &file_key,
                &file_key,
                Some(&target_tagging),
                target_storage_class,
            )
            .await;
    }

    /// Permanently remove the file on S3.
    /// This method should be used by `DfsGc` ONLY to meet GC rules.
    async fn permanently_remove(&self, file_id: u64, opts: Options) -> crate::dfs::Result<()> {
        if self.is_on_aws() || self.is_on_aliyun() {
            return Err(Error::Other(format!(
                "{} permanently_remove is forbidden on AWS or alibaba cloud",
                file_id
            )));
        }

        self.delete_object(self.file_key(file_id, opts.file_type), file_id.to_string())
            .await
    }

    async fn list(
        &self,
        start_after: &str,
        prefix: Option<&str>,
        max_keys: Option<u32>,
    ) -> crate::dfs::Result<(Vec<ListObjectContent>, bool, Option<String>)> {
        self.core.list(start_after, prefix, max_keys).await
    }

    async fn get_object(
        &self,
        key: String,
        file_name: String,
        opts: GetObjectOptions,
    ) -> crate::dfs::Result<Bytes> {
        self.core.get_object(key, file_name, opts).await
    }

    async fn get_object_with_cache(
        &self,
        key: String,
        file_name: String,
        opts: GetObjectOptions,
        cache: Option<&ObjectCacheWithHook>,
    ) -> crate::dfs::Result<Bytes> {
        self.core
            .get_object_with_cache(key, file_name, opts, cache)
            .await
    }

    async fn get_object_to_path(
        &self,
        key: String,
        file_name: String,
        opts: GetObjectOptions,
        path: &Path,
    ) -> crate::dfs::Result<u64> {
        let path = path.to_path_buf();
        let build_writer = move || -> std::io::Result<_> {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .truncate(true)
                .open(&path)?;
            Ok(std::io::BufWriter::new(file))
        };
        let (mut writer, len) = self
            .get_object_to_writer(key, file_name, opts, build_writer)
            .await?;
        writer.flush()?;
        Ok(len)
    }

    async fn put_object(
        &self,
        key: String,
        data: Bytes,
        file_name: String,
    ) -> crate::dfs::Result<()> {
        self.core.put_object(key, data, file_name).await
    }

    async fn put_object_with_storage_class(
        &self,
        key: String,
        data: Bytes,
        file_name: String,
        storage_class: StorageClass,
    ) -> crate::dfs::Result<()> {
        let storage_class = Some(self.storage_class_str(storage_class));
        self.put_object_with_options(key, data, file_name, None, storage_class, None)
            .await
    }

    async fn exist(&self, key: String, file_name: String) -> crate::dfs::Result<bool> {
        self.core.exist(key, file_name).await
    }

    async fn retain_file(&self, file_key: &str) -> crate::dfs::Result<()> {
        self.core.retain_file(file_key).await
    }

    fn list_objects(
        &self,
        start_after: &str,
        prefix: Option<&str>,
        max_keys: Option<u32>,
    ) -> std::result::Result<(Vec<ListObjectContent>, Option<String>), String> {
        ObjectStorage::list_objects(self, start_after, prefix, max_keys)
    }

    fn put_objects(&self, objects: Vec<(String, Bytes)>) -> std::result::Result<(), String> {
        ObjectStorage::put_objects(self, objects)
    }

    fn get_runtime(&self) -> &Runtime {
        self.runtime.as_ref().unwrap()
    }

    fn get_prefix(&self) -> String {
        self.prefix.clone()
    }
}

#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "PascalCase")]
pub struct ListObjects {
    pub common_prefixes: Vec<CommonPrefix>,
    pub contents: Vec<ListObjectContent>,
    pub is_truncated: bool,
}

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "PascalCase")]
pub struct CommonPrefix {
    pub prefix: String,
}

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "PascalCase")]
pub struct Tagging {
    tag_set: TagSet,
}

impl Tagging {
    fn new_single_deleted() -> Tagging {
        Self {
            tag_set: TagSet {
                tag: vec![Tag::new_deleted()],
            },
        }
    }

    pub fn add_tag(&mut self, key: String, value: String) {
        self.tag_set.tag.push(Tag { key, value });
    }

    fn has_deleted_tag(&self) -> bool {
        self.tag_set.tag.iter().any(|tag| tag.is_deleted())
    }

    pub fn to_xml(&self) -> String {
        quick_xml::se::to_string(&self).unwrap()
    }

    pub fn to_url_encoded(&self) -> String {
        let mut se = url::form_urlencoded::Serializer::new(String::new());
        for tag in &self.tag_set.tag {
            se.append_pair(&tag.key, &tag.value);
        }
        se.finish()
    }

    pub fn from_url_encoded(query: &str) -> Tagging {
        let mut tagging = Self::default();
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            tagging.tag_set.tag.push(Tag {
                key: key.into_owned(),
                value: value.into_owned(),
            });
        }
        tagging
    }
}

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "PascalCase")]
struct TagSet {
    tag: Vec<Tag>,
}

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
#[serde(rename_all = "PascalCase")]
struct Tag {
    #[serde(rename = "$unflatten=Key")]
    key: String,
    #[serde(rename = "$unflatten=Value")]
    value: String,
}

impl Tag {
    fn new_deleted() -> Self {
        Self {
            key: "deleted".to_string(),
            value: "true".to_string(),
        }
    }

    fn is_deleted(&self) -> bool {
        self.key == "deleted" && self.value == "true"
    }
}

struct Response {
    resp: HttpResponse,
}

impl Deref for Response {
    type Target = HttpResponse;

    fn deref(&self) -> &Self::Target {
        &self.resp
    }
}

impl DerefMut for Response {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.resp
    }
}

impl Debug for Response {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.resp.status)
    }
}

pub struct OperationVerifier {
    counter: AtomicUsize,
    limit: usize,
}

impl OperationVerifier {
    pub fn new(limit: usize) -> Self {
        Self {
            counter: AtomicUsize::new(0),
            limit,
        }
    }

    pub fn should_verify(&self) -> bool {
        if self.counter.load(Ordering::Relaxed) > self.limit {
            return false;
        }
        let count = self.counter.fetch_add(1, Ordering::Relaxed);
        count <= self.limit
    }
}

#[cfg(any(test, feature = "testexport"))]
pub mod test_util {
    use std::str;

    use http::header;
    use rusoto_mock::{
        MockCredentialsProvider, MockRequestDispatcher, MultipleMockRequestDispatcher,
    };

    use crate::dfs::S3Fs;

    pub fn new_test_s3fs(file_data: &[u8]) -> S3Fs {
        let s3c = rusoto_core::Client::new_with(
            MockCredentialsProvider,
            MultipleMockRequestDispatcher::new(vec![
                MockRequestDispatcher::with_status(200),
                MockRequestDispatcher::with_status(200)
                    .with_header(
                        header::CONTENT_LENGTH.as_str(),
                        &file_data.len().to_string(),
                    )
                    .with_body(str::from_utf8(file_data).unwrap()),
                MockRequestDispatcher::with_status(200),
                MockRequestDispatcher::with_status(200),
            ]),
        );
        S3Fs::new_for_test(s3c, "shard-db".into(), "prefix".into())
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, io::Write};

    use bytes::Buf;

    use super::*;
    use crate::{
        dfs::{FileType, test_util::new_test_s3fs},
        table::{
            file::{File, LocalFile},
            sstable::new_filename,
        },
    };

    #[test]
    fn test_s3() {
        ::test_util::init_log_for_test();

        let local_dir = tempfile::tempdir().unwrap();
        let file_data = "abcdefgh".to_string().into_bytes();
        let s3fs = new_test_s3fs(&file_data);
        let (tx, rx) = tikv_util::mpsc::bounded(1);

        let fs = s3fs.clone();
        let file_data2 = file_data.clone();
        let f = async move {
            match fs
                .create(321, bytes::Bytes::from(file_data2), Options::default())
                .await
            {
                Ok(_) => {
                    tx.send(true).unwrap();
                    println!("create ok");
                }
                Err(err) => {
                    tx.send(false).unwrap();
                    println!("create error {:?}", err)
                }
            }
        };
        s3fs.get_runtime().spawn(f);
        assert!(rx.recv().unwrap());
        let fs = s3fs.clone();
        let (tx, rx) = tikv_util::mpsc::bounded(1);
        let local_file = new_filename(321, local_dir.path());
        let move_local_file = local_file.clone();
        let f = async move {
            let opts = Options::default();
            match fs.read_file(321, opts).await {
                Ok(data) => {
                    let mut file = std::fs::File::create(&move_local_file).unwrap();
                    file.write_all(data.chunk()).unwrap();
                    tx.send(true).unwrap();
                    println!("prefetch ok");
                }
                Err(err) => {
                    tx.send(false).unwrap();
                    println!("prefetch failed {:?}", err)
                }
            }
        };
        s3fs.get_runtime().spawn(f);
        assert!(rx.recv().unwrap());
        let data = std::fs::read(&local_file).unwrap();
        assert_eq!(&data, &file_data);
        let file = LocalFile::open(321, local_file.clone()).unwrap();
        assert_eq!(file.size(), 8u64);
        assert_eq!(file.id(), 321u64);
        let data = file.read(0, 8).unwrap();
        assert_eq!(&data, &file_data);
        let fs = s3fs.clone();
        let (tx, rx) = tikv_util::mpsc::bounded(1);
        let f = async move {
            fs.remove(321, None, Options::default()).await;
            tx.send(true).unwrap();
        };
        s3fs.get_runtime().spawn(f);
        assert!(rx.recv().unwrap());
        let _ = fs::remove_file(local_file);
    }

    #[test]
    fn test_s3_schema_file() {
        ::test_util::init_log_for_test();

        let local_dir = tempfile::tempdir().unwrap();
        let file_data = "abcdefgh".to_string().into_bytes();
        let s3fs = new_test_s3fs(&file_data);
        let (tx, rx) = tikv_util::mpsc::bounded(1);

        let fs = s3fs.clone();
        let file_data2 = file_data.clone();
        let opts = Options::default().with_type(FileType::Schema);
        let f = async move {
            match fs.create(1234, bytes::Bytes::from(file_data2), opts).await {
                Ok(_) => {
                    tx.send(true).unwrap();
                    println!("create ok");
                }
                Err(err) => {
                    tx.send(false).unwrap();
                    println!("create error {:?}", err)
                }
            }
        };
        s3fs.get_runtime().spawn(f);
        assert!(rx.recv().unwrap());
        let fs = s3fs.clone();
        let (tx, rx) = tikv_util::mpsc::bounded(1);
        let local_file = local_dir.path().join(format!("{:016x}.schema", 1234));
        let move_local_file = local_file.clone();
        let f = async move {
            match fs.read_file(1234, opts).await {
                Ok(data) => {
                    let mut file = std::fs::File::create(&move_local_file).unwrap();
                    file.write_all(data.chunk()).unwrap();
                    tx.send(true).unwrap();
                    println!("prefetch ok");
                }
                Err(err) => {
                    tx.send(false).unwrap();
                    println!("prefetch failed {:?}", err)
                }
            }
        };
        s3fs.get_runtime().spawn(f);
        assert!(rx.recv().unwrap());
        let data = std::fs::read(&local_file).unwrap();
        assert_eq!(&data, &file_data);
        let file = LocalFile::open(1234, local_file.clone()).unwrap();
        assert_eq!(file.size(), 8u64);
        assert_eq!(file.id(), 1234u64);
        let data = file.read(0, 8).unwrap();
        assert_eq!(&data, &file_data);
        let fs = s3fs.clone();
        let (tx, rx) = tikv_util::mpsc::bounded(1);
        let f = async move {
            fs.remove(1234, None, opts).await;
            tx.send(true).unwrap();
        };
        s3fs.get_runtime().spawn(f);
        assert!(rx.recv().unwrap());
        let _ = fs::remove_file(local_file);
    }

    #[test]
    fn test_tagging() {
        let tagging_deleted = Tagging::new_single_deleted();

        assert_eq!(
            tagging_deleted.to_xml(),
            "<Tagging><TagSet><Tag><Key>deleted</Key><Value>true</Value></Tag></TagSet></Tagging>"
        );
        assert_eq!(tagging_deleted.to_url_encoded(), "deleted=true");
        assert_eq!(
            tagging_deleted.tag_set.tag,
            Tagging::from_url_encoded("deleted=true").tag_set.tag
        );
    }
}

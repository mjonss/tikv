// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{Debug, Formatter},
    future::Future,
    io::Write,
    num::NonZeroU32,
    path::Path,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use azure_core::{
    Error as AzureError, ExponentialRetryOptions, RetryOptions,
    error::ErrorKind,
    prelude::{Delimiter, MaxResults, NextMarker, Prefix, Range},
};
use azure_storage::{CloudLocation, ConnectionString, EndpointProtocol, StorageCredentials};
use azure_storage_blobs::prelude::{AccessTier, BlobClient, ClientBuilder, ContainerClient, Tags};
use bytes::{BufMut, Bytes, BytesMut};
use engine_traits::{GetObjectOptions, ListObjectContent, ObjectCacheWithHook, ObjectStorage};
use futures::StreamExt;
use http::StatusCode;
use tikv_util::time::Instant;
use tokio::runtime::Runtime;

use crate::dfs::{
    Dfs, Error, Options, ReservableWriter, STORAGE_CLASS_DEFAULT, STORAGE_CLASS_GLACIER_IR,
    STORAGE_CLASS_INTELLIGENT_TIERING, STORAGE_CLASS_STANDARD, STORAGE_CLASS_STANDARD_IA,
    StorageClass,
    config::{AzureConfig, Config, ConnOptions},
    metrics::*,
};

#[cfg(test)]
const DEVELOPMENT_STORAGE_CONNECTION_STRING: &str = "UseDevelopmentStorage=true";
const TAG_DELETED_KEY: &str = "deleted";
const MAX_DELAY_DURATION: Duration = Duration::from_secs(00);

#[derive(Clone)]
pub struct AzureFs {
    core: Arc<AzureFsCore>,
}

impl AzureFs {
    pub fn new_from_config(conf: Config) -> Self {
        let core = AzureFsCore::new(conf).unwrap_or_else(|err| {
            panic!("failed to initialize azure dfs: {:?}", err);
        });
        Self {
            core: Arc::new(core),
        }
    }
}

impl std::ops::Deref for AzureFs {
    type Target = AzureFsCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

pub struct AzureFsCore {
    container_client: Arc<ContainerClient>,
    prefix: String,
    runtime: Option<Runtime>,
    read_only: bool,
    opts: ConnOptions,
}

impl AzureFsCore {
    fn new(conf: Config) -> crate::dfs::Result<Self> {
        let Config {
            azure,
            mut prefix,
            read_only,
            conn_options,
            ..
        } = conf;
        if azure.container.is_empty() {
            return Err(Error::Other(
                "azure container is required for dfs azure backend".to_string(),
            ));
        }

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("azure")
            .build()
            .unwrap();

        let retry_options = Self::retry_options(&conn_options);
        let container_client = Self::build_container_client(&azure, &retry_options)?;
        if prefix.is_empty() {
            prefix.push_str("default");
        }

        let core = Self {
            container_client,
            prefix,
            runtime: Some(runtime),
            read_only,
            opts: conn_options,
        };
        core.ensure_container();
        Ok(core)
    }

    fn retry_options(opts: &ConnOptions) -> RetryOptions {
        RetryOptions::exponential(
            ExponentialRetryOptions::default()
                .initial_delay(opts.retry_sleep_interval.0)
                .max_retries(opts.max_retry_count)
                .max_delay(MAX_DELAY_DURATION),
        )
    }

    fn build_container_client(
        azure: &AzureConfig,
        retry_options: &RetryOptions,
    ) -> crate::dfs::Result<Arc<ContainerClient>> {
        let container = azure.container.clone();
        if !azure.connection_string.is_empty() {
            let connection_string = ConnectionString::new(azure.connection_string.as_str())
                .map_err(|e| Error::Other(format!("azure connection string error: {}", e)))?;
            if connection_string.use_development_storage == Some(true) {
                return Ok(Arc::new(
                    ClientBuilder::emulator()
                        .retry(retry_options.clone())
                        .container_client(container),
                ));
            }
            let account_name = connection_string.account_name.ok_or_else(|| {
                Error::Other("azure connection string missing account name".to_string())
            })?;
            let credentials = connection_string
                .storage_credentials()
                .map_err(|e| Error::Other(format!("azure connection string error: {}", e)))?;
            let cloud_location =
                Self::cloud_location_from_connection_string(&connection_string, account_name);
            let container_client = ClientBuilder::with_location(cloud_location, credentials)
                .retry(retry_options.clone())
                .container_client(container);
            return Ok(Arc::new(container_client));
        }

        if !azure.account_name.is_empty() && !azure.account_key.is_empty() {
            let credentials = StorageCredentials::access_key(
                azure.account_name.clone(),
                azure.account_key.clone(),
            );
            let cloud_location = if azure.endpoint.is_empty() {
                CloudLocation::Public {
                    account: azure.account_name.clone(),
                }
            } else {
                CloudLocation::Custom {
                    account: azure.account_name.clone(),
                    uri: azure.endpoint.clone(),
                }
            };
            let container_client = ClientBuilder::with_location(cloud_location, credentials)
                .retry(retry_options.clone())
                .container_client(container);
            return Ok(Arc::new(container_client));
        }

        Err(Error::Other(
            "azure dfs backend requires connection string or account name/key".to_string(),
        ))
    }

    fn cloud_location_from_connection_string(
        connection_string: &ConnectionString<'_>,
        account_name: &str,
    ) -> CloudLocation {
        if let Some(blob_endpoint) = connection_string.blob_endpoint {
            return CloudLocation::Custom {
                account: account_name.to_string(),
                uri: blob_endpoint.to_string(),
            };
        }
        if connection_string.endpoint_suffix.is_some()
            || connection_string.default_endpoints_protocol.is_some()
        {
            let protocol = match connection_string
                .default_endpoints_protocol
                .as_ref()
                .unwrap_or(&EndpointProtocol::Https)
            {
                EndpointProtocol::Http => "http",
                EndpointProtocol::Https => "https",
            };
            let endpoint_suffix = connection_string
                .endpoint_suffix
                .unwrap_or("core.windows.net");
            return CloudLocation::Custom {
                account: account_name.to_string(),
                uri: format!("{protocol}://{account_name}.blob.{endpoint_suffix}"),
            };
        }
        CloudLocation::Public {
            account: account_name.to_string(),
        }
    }

    fn ensure_container(&self) {
        let runtime = self.runtime.as_ref().unwrap();
        runtime.block_on(async {
            let result = self
                .timeout_or_io_err(self.opts.dispatch_timeout.0, async {
                    self.container_client.create().await
                })
                .await;
            match result {
                Ok(_) => {}
                Err(err) if is_status_code(&err, StatusCode::CONFLICT) => {}
                Err(err) => {
                    warn!("failed to create azure container: {}", err);
                }
            }
        });
    }

    fn blob_client(&self, key: &str) -> BlobClient {
        self.container_client.blob_client(key)
    }

    async fn timeout_or_io_err<T, F>(&self, timeout: Duration, fut: F) -> Result<T, AzureError>
    where
        F: Future<Output = Result<T, AzureError>>,
    {
        match tokio::time::timeout(timeout, fut).await {
            Ok(result) => result,
            Err(_) => Err(AzureError::message(
                ErrorKind::Io,
                format!("azure request timeout after {:?}", timeout),
            )),
        }
    }

    async fn object_size_inner(&self, key: &str) -> crate::dfs::Result<u64> {
        let result = self
            .timeout_or_io_err(self.opts.dispatch_timeout.0, async {
                self.blob_client(key).get_properties().await
            })
            .await;
        match result {
            Ok(resp) => Ok(resp.blob.properties.content_length),
            Err(err) if is_status_code(&err, StatusCode::NOT_FOUND) => {
                Err(Error::NoSuchKey(key.to_string()))
            }
            Err(err) => Err(Error::Other(format!("azure head error: {}", err))),
        }
    }

    async fn get_range(
        &self,
        key: &str,
        opts: GetObjectOptions,
    ) -> crate::dfs::Result<Option<Range>> {
        if opts.is_full_range() {
            return Ok(None);
        }
        if let (Some(start_off), Some(end_off)) = (opts.start_off, opts.end_off) {
            return Ok(Some(Range::new(start_off, end_off)));
        }
        let range = match (opts.start_off, opts.end_off) {
            (Some(start_off), None) => (start_off..).into(),
            (None, Some(len)) => {
                let size = self.object_size_inner(key).await?;
                let start = size.saturating_sub(len);
                Range::new(start, size)
            }
            _ => return Ok(None),
        };
        Ok(Some(range))
    }

    pub async fn get_object_to_writer<W, F>(
        &self,
        key: String,
        _file_name: String,
        opts: GetObjectOptions,
        build_writer: F,
    ) -> crate::dfs::Result<(W, u64)>
    where
        W: ReservableWriter + Send + 'static,
        F: Fn() -> std::io::Result<W> + 'static,
    {
        let start_time = Instant::now_coarse();
        let range = self.get_range(&key, opts).await?;
        let blob_client = self.blob_client(&key);
        let builder = if let Some(range) = range {
            blob_client.get().range(range)
        } else {
            blob_client.get()
        };
        let mut stream = builder.into_stream();
        let mut writer = build_writer()?;
        let mut read_len = 0;
        let mut err = None;

        loop {
            let response = match self
                .timeout_or_io_err(self.opts.dispatch_timeout.0, async {
                    stream.next().await.transpose()
                })
                .await
            {
                Ok(Some(response)) => response,
                Ok(None) => break,
                Err(e) => {
                    err = Some(e);
                    break;
                }
            };

            let content_len = response.blob.properties.content_length;
            writer.reserve_capacity(content_len);
            let mut body = response.data;
            let response_len = {
                let writer = &mut writer;
                self.timeout_or_io_err(self.opts.read_body_timeout.0, async move {
                    let mut response_len = 0;
                    while let Some(chunk) = body.next().await.transpose()? {
                        response_len += chunk.len() as u64;
                        writer.write_all(&chunk).map_err(|err| {
                            AzureError::message(ErrorKind::Io, format!("write_all: {}", err))
                        })?;
                    }
                    Ok(response_len)
                })
                .await
            };
            match response_len {
                Ok(response_len) => {
                    read_len += response_len;
                }
                Err(e) => {
                    err = Some(e);
                    break;
                }
            }
        }

        if let Some(err) = err {
            if is_status_code(&err, StatusCode::NOT_FOUND) {
                return Err(Error::NoSuchKey(key));
            }
            return Err(Error::Other(format!("azure get error: {}", err)));
        }

        KVENGINE_DFS_THROUGHPUT_VEC
            .with_label_values(&["read"])
            .inc_by(read_len);
        KVENGINE_DFS_LATENCY_VEC
            .with_label_values(&["read"])
            .observe(start_time.saturating_elapsed().as_millis() as f64);
        Ok((writer, read_len))
    }

    async fn put_object_with_tier(
        &self,
        key: String,
        data: Bytes,
        access_tier: Option<AccessTier>,
    ) -> crate::dfs::Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        let start_time = Instant::now_coarse();
        let data_len = data.len();
        let data_mb = data_len as u32 / (1024 * 1024);
        let timeout = self.opts.dispatch_timeout.0 + self.opts.write_timeout_per_mb.0 * data_mb;
        let blob_client = self.blob_client(&key);
        let mut builder = blob_client.put_block_blob(data);
        if let Some(access_tier) = access_tier {
            builder = builder.access_tier(access_tier);
        }
        let result = self
            .timeout_or_io_err(timeout, async { builder.await })
            .await;
        match result {
            Ok(_) => {
                KVENGINE_DFS_THROUGHPUT_VEC
                    .with_label_values(&["write"])
                    .inc_by(data_len as u64);
                KVENGINE_DFS_LATENCY_VEC
                    .with_label_values(&["write"])
                    .observe(start_time.saturating_elapsed().as_millis() as f64);
                Ok(())
            }
            Err(err) => Err(Error::Other(format!("azure put error: {}", err))),
        }
    }

    async fn set_deleted_tag(&self, key: &str, deleted: bool) -> crate::dfs::Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        let blob_client = self.blob_client(key);
        let mut tags = match self
            .timeout_or_io_err(self.opts.dispatch_timeout.0, async {
                blob_client.get_tags().await
            })
            .await
        {
            Ok(resp) => resp.tags,
            Err(err) if is_status_code(&err, StatusCode::NOT_FOUND) => Tags::new(),
            Err(err) => return Err(Error::Other(format!("azure get tags error: {}", err))),
        };
        if deleted {
            let mut updated = false;
            for tag in tags.tag_set.tags.iter_mut() {
                if tag.key == TAG_DELETED_KEY {
                    tag.value = "true".to_string();
                    updated = true;
                    break;
                }
            }
            if !updated {
                tags.insert(TAG_DELETED_KEY, "true");
            }
        } else {
            tags.tag_set.tags.retain(|tag| tag.key != TAG_DELETED_KEY);
        }
        let result = self
            .timeout_or_io_err(self.opts.dispatch_timeout.0, async {
                blob_client.set_tags(tags).await
            })
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(err) => Err(Error::Other(format!("azure set tags error: {}", err))),
        }
    }

    async fn get_tag_deleted(&self, key: &str) -> crate::dfs::Result<bool> {
        let blob_client = self.blob_client(key);
        let result = self
            .timeout_or_io_err(self.opts.dispatch_timeout.0, async {
                blob_client.get_tags().await
            })
            .await;
        match result {
            Ok(resp) => Ok(resp
                .tags
                .tag_set
                .tags
                .iter()
                .find(|tag| tag.key == TAG_DELETED_KEY)
                .map(|tag| tag.value == "true")
                .unwrap_or(false)),
            Err(err) if is_status_code(&err, StatusCode::NOT_FOUND) => Ok(false),
            Err(err) => Err(Error::Other(format!("azure get tags error: {}", err))),
        }
    }

    fn storage_class_str(storage_class: StorageClass) -> &'static str {
        match storage_class {
            StorageClass::Standard => STORAGE_CLASS_STANDARD,
            StorageClass::InfrequentAccess => STORAGE_CLASS_STANDARD_IA,
            StorageClass::GlacierInstantRetrieval => STORAGE_CLASS_GLACIER_IR,
            StorageClass::IntelligentTiering => STORAGE_CLASS_INTELLIGENT_TIERING,
            StorageClass::Default => STORAGE_CLASS_DEFAULT,
        }
    }

    fn storage_class_from_tier(tier: Option<AccessTier>) -> StorageClass {
        match tier {
            Some(AccessTier::Hot) => StorageClass::Standard,
            Some(AccessTier::Cool) => StorageClass::InfrequentAccess,
            Some(AccessTier::Archive) => StorageClass::GlacierInstantRetrieval,
            None => StorageClass::Default,
        }
    }

    fn access_tier_from_storage_class(storage_class: StorageClass) -> Option<AccessTier> {
        match storage_class {
            StorageClass::InfrequentAccess => Some(AccessTier::Cool),
            StorageClass::GlacierInstantRetrieval => Some(AccessTier::Archive),
            StorageClass::Standard | StorageClass::IntelligentTiering | StorageClass::Default => {
                Some(AccessTier::Hot)
            }
        }
    }
}

impl Drop for AzureFsCore {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            std::thread::spawn(|| drop(runtime));
        }
    }
}

impl Debug for AzureFs {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzureFs")
            .field("prefix", &self.prefix)
            .finish()
    }
}

impl ObjectStorage for AzureFs {
    fn put_objects(&self, objects: Vec<(String, Bytes)>) -> Result<(), String> {
        let put_object = |key: String, data: Bytes| {
            let full_key = format!("{}/{}", self.prefix, key);
            let fs = self.clone();
            async move {
                fs.put_object(full_key, data, key.clone())
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
                let res = res.map_err(|e| e.to_string())??;
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
impl Dfs for AzureFs {
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
        self.put_object_with_storage_class(
            self.file_key(file_id, opts.file_type),
            data,
            format!("{}.{}", file_id, opts.file_type.suffix()),
            StorageClass::Default,
        )
        .await
    }

    async fn remove(&self, file_id: u64, _file_len: Option<u64>, opts: Options) {
        let file_key = self.file_key(file_id, opts.file_type);
        if let Err(err) = self.set_deleted_tag(&file_key, true).await {
            error!("failed to mark azure object removed: {:?}", err);
        }
    }

    async fn permanently_remove(&self, file_id: u64, opts: Options) -> crate::dfs::Result<()> {
        self.delete_object(self.file_key(file_id, opts.file_type), file_id.to_string())
            .await
    }

    async fn list(
        &self,
        start_after: &str,
        prefix: Option<&str>,
        max_keys: Option<u32>,
    ) -> crate::dfs::Result<(Vec<ListObjectContent>, bool, Option<String>)> {
        let prefix = format!("{}/{}", self.prefix, prefix.unwrap_or_default());
        let start_after = format!("{}{}", prefix, start_after);
        let mut builder = self
            .container_client
            .list_blobs()
            .prefix(Prefix::new(prefix.clone()));
        if let Some(max_keys) = max_keys {
            if let Some(max_results) = NonZeroU32::new(max_keys) {
                builder = builder.max_results(MaxResults::new(max_results));
            }
        }
        if !start_after.is_empty() {
            builder = builder.marker(NextMarker::new(start_after.clone()));
        }
        let mut stream = builder.into_stream();
        let response = match self
            .timeout_or_io_err(self.opts.dispatch_timeout.0, async {
                stream.next().await.transpose()
            })
            .await
        {
            Ok(Some(resp)) => resp,
            Ok(None) => return Ok((vec![], false, None)),
            Err(err) if is_status_code(&err, StatusCode::NOT_FOUND) => {
                return Ok((vec![], false, None));
            }
            Err(err) => return Err(Error::Other(format!("azure list error: {}", err))),
        };

        let contents = response
            .blobs
            .blobs()
            .map(|blob| ListObjectContent {
                key: blob.name.clone(),
                last_modified: azure_core::date::to_rfc3339(&blob.properties.last_modified),
                storage_class: AzureFsCore::storage_class_str(
                    AzureFsCore::storage_class_from_tier(blob.properties.access_tier),
                )
                .to_string(),
                size: blob.properties.content_length,
            })
            .collect::<Vec<_>>();

        let next_start_after = response.next_marker.as_ref().map(|marker| {
            let marker = marker.as_str();
            marker.strip_prefix(&prefix).unwrap_or(marker).to_string()
        });
        let has_more = next_start_after.is_some();
        Ok((contents, has_more, next_start_after))
    }

    async fn get_object(
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
        _file_name: String,
    ) -> crate::dfs::Result<()> {
        self.put_object_with_tier(key, data, None).await
    }

    async fn put_object_with_storage_class(
        &self,
        key: String,
        data: Bytes,
        _file_name: String,
        storage_class: StorageClass,
    ) -> crate::dfs::Result<()> {
        let access_tier = AzureFsCore::access_tier_from_storage_class(storage_class);
        self.put_object_with_tier(key, data, access_tier).await
    }

    async fn exist(&self, key: String, _file_name: String) -> crate::dfs::Result<bool> {
        match self.object_size_inner(&key).await {
            Ok(_) => Ok(true),
            Err(Error::NoSuchKey(_)) => Ok(false),
            Err(err) => Err(err),
        }
    }

    async fn retain_file(&self, file_key: &str) -> crate::dfs::Result<()> {
        self.set_deleted_tag(file_key, false).await
    }

    async fn delete_object(&self, key: String, _file_name: String) -> crate::dfs::Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        let result = self
            .timeout_or_io_err(self.opts.dispatch_timeout.0, async {
                self.blob_client(&key).delete().await
            })
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(err) if is_status_code(&err, StatusCode::NOT_FOUND) => Ok(()),
            Err(err) => Err(Error::Other(format!("azure delete error: {}", err))),
        }
    }

    async fn object_size(&self, key: String, _file_name: String) -> crate::dfs::Result<u64> {
        self.object_size_inner(&key).await
    }

    async fn list_folders(
        &self,
        prefix: &str,
        delimiter: Option<&str>,
    ) -> crate::dfs::Result<Vec<String>> {
        let delimiter = delimiter.unwrap_or("/");
        let prefix = format!("{}/{}", self.prefix, prefix);
        let mut folders = Vec::new();
        let builder = self
            .container_client
            .list_blobs()
            .prefix(Prefix::new(prefix.clone()))
            .delimiter(Delimiter::new(delimiter.to_string()));
        let mut stream = builder.into_stream();
        let mut err = None;
        loop {
            let response = match self
                .timeout_or_io_err(self.opts.dispatch_timeout.0, async {
                    stream.next().await.transpose()
                })
                .await
            {
                Ok(Some(resp)) => resp,
                Ok(None) => break,
                Err(e) => {
                    err = Some(e);
                    break;
                }
            };
            folders.extend(response.blobs.prefixes().map(|p| p.name.clone()));
        }
        if let Some(err) = err {
            if is_status_code(&err, StatusCode::NOT_FOUND) {
                return Ok(vec![]);
            }
            return Err(Error::Other(format!("azure list error: {}", err)));
        }
        Ok(folders)
    }

    async fn is_removed(&self, file_key: &str) -> crate::dfs::Result<bool> {
        self.get_tag_deleted(file_key).await
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

fn is_status_code(err: &(dyn std::error::Error + 'static), status: StatusCode) -> bool {
    let Some(azure_err) = err.downcast_ref::<AzureError>() else {
        return false;
    };
    if let Some(http_err) = azure_err.as_http_error() {
        return http_err.status() == status.as_u16();
    }
    match azure_err.kind() {
        ErrorKind::HttpResponse {
            status: err_status, ..
        } => *err_status == status.as_u16(),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use bytes::Bytes;
    use engine_traits::{GetObjectOptions, ObjectCache, ObjectCacheWithHook};
    use rand::Rng;

    use super::DEVELOPMENT_STORAGE_CONNECTION_STRING;
    use crate::dfs::{
        FileType, Options, StorageClass,
        config::{AzureConfig, Config},
        new_dfs_from_config,
    };

    fn azurite_config() -> Config {
        let mut rng = rand::thread_rng();
        let suffix = rng.gen::<u64>();
        Config {
            backend: "azure".to_string(),
            prefix: format!("azurite-{}", suffix),
            azure: AzureConfig {
                connection_string: DEVELOPMENT_STORAGE_CONNECTION_STRING.to_string(),
                container: format!("azurite-{}", suffix),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn test_azurite() {
        test_util::init_log_for_test();
        if std::env::var("TIKV_AZURITE_TEST").ok().as_deref() != Some("1") {
            eprintln!("skip azurite test; set TIKV_AZURITE_TEST=1 to enable");
            return;
        }
        std::env::set_var("TIKV_DISABLE_SYSTEM_PROXY", "1");

        let dfs = new_dfs_from_config(azurite_config());
        let runtime = dfs.get_runtime();

        let file_data = Bytes::from_static(b"azurite-file");
        runtime
            .block_on(dfs.create(1, file_data.clone(), Options::default()))
            .unwrap();
        let read_data = runtime
            .block_on(dfs.read_file(1, Options::default()))
            .unwrap();
        assert_eq!(read_data, file_data);

        let delete_data = Bytes::from_static(b"azurite-delete");
        runtime
            .block_on(dfs.create(2, delete_data, Options::default()))
            .unwrap();
        runtime
            .block_on(dfs.permanently_remove(2, Options::default()))
            .unwrap();

        let object_key = format!("{}/objects/test", dfs.get_prefix());
        let object_data = Bytes::from_static(b"azurite-object");
        runtime
            .block_on(dfs.put_object_with_storage_class(
                object_key.clone(),
                object_data.clone(),
                "test".to_string(),
                StorageClass::Standard,
            ))
            .unwrap();

        let plain_key = format!("{}/objects/plain", dfs.get_prefix());
        let plain_data = Bytes::from_static(b"azurite-plain");
        runtime
            .block_on(dfs.put_object(plain_key.clone(), plain_data.clone(), "plain".to_string()))
            .unwrap();
        let plain_fetched = runtime
            .block_on(dfs.get_object(
                plain_key.clone(),
                "plain".to_string(),
                GetObjectOptions::default(),
            ))
            .unwrap();
        assert_eq!(plain_fetched, plain_data);

        let exists = runtime
            .block_on(dfs.exist(object_key.clone(), "test".to_string()))
            .unwrap();
        assert!(exists);
        let size = runtime
            .block_on(dfs.object_size(object_key.clone(), "test".to_string()))
            .unwrap();
        assert_eq!(size as usize, object_data.len());

        let fetched = runtime
            .block_on(dfs.get_object(
                object_key.clone(),
                "test".to_string(),
                GetObjectOptions::default(),
            ))
            .unwrap();
        assert_eq!(fetched, object_data);

        let range_key = format!("{}/objects/range", dfs.get_prefix());
        let range_data = Bytes::from_static(b"0123456789abcdef");
        runtime
            .block_on(dfs.put_object(range_key.clone(), range_data.clone(), "range".to_string()))
            .unwrap();
        let range_fetched = runtime
            .block_on(dfs.get_object(
                range_key,
                "range".to_string(),
                GetObjectOptions {
                    start_off: Some(2),
                    end_off: Some(6),
                },
            ))
            .unwrap();
        assert_eq!(range_fetched, Bytes::from_static(b"2345"));

        let hook_calls = Arc::new(AtomicUsize::new(0));
        let hook_calls_clone = Arc::clone(&hook_calls);
        let cache = ObjectCache::new(1024 * 1024, 256);
        let cache = ObjectCacheWithHook::from(cache).with_hook(Arc::new(move |data| {
            hook_calls_clone.fetch_add(1, Ordering::SeqCst);
            Ok(data)
        }));
        let cached = runtime
            .block_on(dfs.get_object_with_cache(
                object_key.clone(),
                "test".to_string(),
                GetObjectOptions::default(),
                Some(&cache),
            ))
            .unwrap();
        assert_eq!(cached, object_data);
        let cached_again = runtime
            .block_on(dfs.get_object_with_cache(
                object_key.clone(),
                "test".to_string(),
                GetObjectOptions::default(),
                Some(&cache),
            ))
            .unwrap();
        assert_eq!(cached_again, object_data);
        assert_eq!(hook_calls.load(Ordering::SeqCst), 1);

        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("downloaded");
        let saved = runtime
            .block_on(dfs.get_object_to_path(
                object_key.clone(),
                "test".to_string(),
                GetObjectOptions::default(),
                &path,
            ))
            .unwrap();
        assert_eq!(saved as usize, object_data.len());
        assert_eq!(std::fs::read(&path).unwrap(), object_data);

        dfs.put_objects(vec![
            (
                "folders/alpha/item".to_string(),
                Bytes::from_static(b"alpha"),
            ),
            ("folders/beta/item".to_string(), Bytes::from_static(b"beta")),
        ])
        .unwrap();

        let folders = runtime
            .block_on(dfs.list_folders("folders/", None))
            .unwrap();
        assert!(
            folders
                .iter()
                .any(|folder| folder.contains("folders/alpha"))
        );
        assert!(folders.iter().any(|folder| folder.contains("folders/beta")));

        let (objects, ..) = runtime
            .block_on(dfs.list("", Some("objects/"), None))
            .unwrap();
        assert!(objects.iter().any(|o| o.key.ends_with("objects/test")));

        let (listed, _) = dfs.list_objects("", Some("objects/"), None).unwrap();
        assert!(listed.iter().any(|o| o.key.ends_with("objects/test")));

        let file_key = dfs.file_key(1, FileType::Sst);
        runtime.block_on(async {
            dfs.remove(1, None, Options::default()).await;
        });
        let removed = runtime.block_on(dfs.is_removed(&file_key)).unwrap();
        assert!(removed);
        runtime.block_on(dfs.retain_file(&file_key)).unwrap();
        let removed = runtime.block_on(dfs.is_removed(&file_key)).unwrap();
        assert!(!removed);

        runtime
            .block_on(dfs.delete_object(object_key, "test".to_string()))
            .unwrap();
    }
}

// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    io::{self, Error, ErrorKind},
    sync::Arc,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cloud::metrics;
use futures::{Future, future::TryFutureExt};
use rusoto_core::{
    region::Region,
    request::{HttpClient, HttpConfig},
};
use rusoto_credential::{
    AutoRefreshingProvider, AwsCredentials, ChainProvider, CredentialsError, ProvideAwsCredentials,
};
use rusoto_sts::WebIdentityProvider;
use tikv_util::{
    stream::{RetryError, RetryExt, retry_ext},
    warn,
};

#[allow(dead_code)] // This will be used soon, please remove the allow.
const READ_BUF_SIZE: usize = 1024 * 1024 * 2;

const AWS_WEB_IDENTITY_TOKEN_FILE: &str = "AWS_WEB_IDENTITY_TOKEN_FILE";
struct CredentialsErrorWrapper(CredentialsError);

impl From<CredentialsErrorWrapper> for CredentialsError {
    fn from(c: CredentialsErrorWrapper) -> CredentialsError {
        c.0
    }
}

impl std::fmt::Display for CredentialsErrorWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.message)?;
        Ok(())
    }
}

impl RetryError for CredentialsErrorWrapper {
    fn is_retryable(&self) -> bool {
        true
    }
}

pub fn new_http_client() -> io::Result<HttpClient> {
    let mut http_config = HttpConfig::new();
    // This can greatly improve performance dealing with payloads greater
    // than 100MB. See https://github.com/rusoto/rusoto/pull/1227
    // for more information.
    http_config.read_buf_size(READ_BUF_SIZE);
    // It is important to explicitly create the client and not use a global
    // See https://github.com/tikv/tikv/issues/7236.
    HttpClient::new_with_config(http_config).map_err(|e| {
        Error::new(
            ErrorKind::Other,
            format!("create aws http client error: {}", e),
        )
    })
}

pub fn get_region(region: &str, endpoint: &str) -> io::Result<Region> {
    if !endpoint.is_empty() {
        Ok(Region::Custom {
            name: region.to_owned(),
            endpoint: endpoint.to_owned(),
        })
    } else if !region.is_empty() {
        region.parse::<Region>().map_err(|e| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("invalid aws region format {}: {}", region, e),
            )
        })
    } else {
        Ok(Region::default())
    }
}

pub async fn retry_and_count<G, T, F, E>(action: G, name: &'static str) -> Result<T, E>
where
    G: FnMut() -> F,
    F: Future<Output = Result<T, E>>,
    E: RetryError + std::fmt::Display,
{
    let id = uuid::Uuid::new_v4();
    retry_ext(
        action,
        RetryExt::default().with_fail_hook(move |err: &E| {
            warn!("aws request meet error."; "err" => %err, "retry?" => %err.is_retryable(), "context" => %name, "uuid" => %id);
            metrics::CLOUD_ERROR_VEC.with_label_values(&["aws", name]).inc();
        }),
    ).await
}

pub struct CredentialsProvider(AutoRefreshingProvider<DefaultCredentialsProvider>);

impl CredentialsProvider {
    pub fn new() -> io::Result<CredentialsProvider> {
        Ok(CredentialsProvider(
            AutoRefreshingProvider::new(DefaultCredentialsProvider::default()).map_err(|e| {
                Error::new(
                    ErrorKind::Other,
                    format!("create aws credentials provider error: {}", e),
                )
            })?,
        ))
    }
}

#[async_trait]
impl ProvideAwsCredentials for CredentialsProvider {
    async fn credentials(&self) -> Result<AwsCredentials, CredentialsError> {
        self.0.credentials().await
    }
}

// Same as rusoto_credentials::DefaultCredentialsProvider with extra
// rusoto_sts::WebIdentityProvider support.
pub struct DefaultCredentialsProvider {
    // Underlying implementation of rusoto_credentials::DefaultCredentialsProvider.
    default_provider: ChainProvider,
    // Provider IAM support in Kubernetes.
    web_identity_provider: WebIdentityProvider,
}

impl Default for DefaultCredentialsProvider {
    fn default() -> DefaultCredentialsProvider {
        DefaultCredentialsProvider {
            default_provider: ChainProvider::new(),
            web_identity_provider: WebIdentityProvider::from_k8s_env(),
        }
    }
}

#[async_trait]
impl ProvideAwsCredentials for DefaultCredentialsProvider {
    async fn credentials(&self) -> Result<AwsCredentials, CredentialsError> {
        // use web identity provider first for the kubernetes environment.
        let cred = if std::env::var(AWS_WEB_IDENTITY_TOKEN_FILE).is_ok() {
            // we need invoke assume_role in web identity provider
            // this API may failed sometimes.
            // according to AWS experience, it's better to retry it with 10 times
            // exponential backoff for every error, because we cannot
            // distinguish the error type.
            retry_and_count(
                || {
                    #[cfg(test)]
                    fail::fail_point!("cred_err", |_| {
                        Box::pin(futures::future::err(CredentialsErrorWrapper(
                            CredentialsError::new("injected error"),
                        )))
                            as std::pin::Pin<Box<dyn futures::Future<Output = _> + Send>>
                    });
                    let res = self
                        .web_identity_provider
                        .credentials()
                        .map_err(|e| CredentialsErrorWrapper(e));
                    #[cfg(test)]
                    return Box::pin(res);
                    #[cfg(not(test))]
                    res
                },
                "get_cred_over_the_cloud",
            )
            .await
            .map_err(|e| e.0)
        } else {
            // Add exponential backoff for every error, because we cannot
            // distinguish the error type.
            retry_and_count(
                || {
                    self.default_provider
                        .credentials()
                        .map_err(|e| CredentialsErrorWrapper(e))
                },
                "get_cred_on_premise",
            )
            .await
            .map_err(|e| e.0)
        };

        cred.map_err(|e| {
            CredentialsError::new(format_args!(
                "Couldn't find AWS credentials in sources ({}).",
                e.message
            ))
        })
    }
}

/// ActiveRefreshingProvider try to refresh the credentials much early than the
/// expiration time. So we can tolerate credential service down for half of the
/// expire duration.
#[derive(Clone)]
pub struct ActiveRefreshingProvider {
    inner: Arc<dyn ProvideAwsCredentials + Send + Sync>,
    state: Arc<tokio::sync::Mutex<CredentialsState>>,
}

struct CredentialsState {
    current: Option<AwsCredentials>,
    refresh_at: Option<DateTime<Utc>>,
}

impl CredentialsState {
    fn set_credentials(&mut self, creds: AwsCredentials) {
        self.refresh_at = creds.expires_at().map(|expire_at| {
            let now = Utc::now();
            let expire_secs = expire_at - now;
            now + expire_secs / 2
        });
        self.current = Some(creds);
    }

    fn need_refresh(&self) -> bool {
        self.refresh_at.map(|t| t < Utc::now()).unwrap_or_default()
    }

    fn is_expired(&self) -> bool {
        match self.current.as_ref() {
            None => true,
            Some(creds) => {
                if let Some(expired_at) = creds.expires_at() {
                    *expired_at < Utc::now()
                } else {
                    false
                }
            }
        }
    }
}

impl ActiveRefreshingProvider {
    pub fn new(inner: Arc<dyn ProvideAwsCredentials + Send + Sync>) -> ActiveRefreshingProvider {
        ActiveRefreshingProvider {
            inner,
            state: Arc::new(tokio::sync::Mutex::new(CredentialsState {
                current: None,
                refresh_at: None,
            })),
        }
    }
}

#[async_trait]
impl ProvideAwsCredentials for ActiveRefreshingProvider {
    async fn credentials(&self) -> Result<AwsCredentials, CredentialsError> {
        let mut state = self.state.lock().await;
        if state.is_expired() {
            let creds = self.inner.credentials().await?;
            state.set_credentials(creds.clone());
        } else if state.need_refresh() {
            let creds_res = self.inner.credentials().await;
            match creds_res {
                Ok(creds) => {
                    state.set_credentials(creds.clone());
                }
                Err(err) => {
                    warn!("failed to refresh credentials {:?}", err);
                    state.refresh_at = Some(Utc::now() + chrono::Duration::seconds(3));
                }
            }
        }
        Ok(state.current.as_ref().unwrap().clone())
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Mutex, time::Duration};

    #[allow(unused_imports)]
    use super::*;

    #[cfg(feature = "failpoints")]
    #[tokio::test]
    async fn test_default_provider() {
        let default_provider = DefaultCredentialsProvider::default();
        std::env::set_var(AWS_WEB_IDENTITY_TOKEN_FILE, "tmp");
        // mock k8s env with web_identitiy_provider
        fail::cfg("cred_err", "return").unwrap();
        fail::cfg("retry_count", "return(1)").unwrap();
        let res = default_provider.credentials().await;
        assert_eq!(res.is_err(), true);
        assert_eq!(
            res.err().unwrap().message,
            "Couldn't find AWS credentials in sources (injected error)."
        );
        fail::remove("cred_err");
        fail::remove("retry_count");

        std::env::remove_var(AWS_WEB_IDENTITY_TOKEN_FILE);
    }

    #[derive(Clone)]
    struct MockProvider {
        creds: Arc<Mutex<Option<AwsCredentials>>>,
    }

    impl MockProvider {
        fn new() -> Self {
            MockProvider {
                creds: Arc::new(Mutex::new(None)),
            }
        }

        fn set_credentials(&self, creds: Option<AwsCredentials>) {
            let mut guard = self.creds.lock().unwrap();
            *guard = creds;
        }
    }

    #[async_trait]
    impl ProvideAwsCredentials for MockProvider {
        async fn credentials(&self) -> Result<AwsCredentials, CredentialsError> {
            let creds = self.creds.lock().unwrap();
            if creds.is_none() {
                return Err(CredentialsError::new("no credentials"));
            }
            Ok(creds.as_ref().unwrap().clone())
        }
    }

    fn new_mock_credential(expire_at: DateTime<Utc>) -> AwsCredentials {
        AwsCredentials::new(
            "access_key".to_string(),
            "access_secret".to_string(),
            None,
            Some(expire_at),
        )
    }

    #[tokio::test]
    async fn test_active_refreshing_provider() {
        let mock_provider = MockProvider::new();
        let provider = ActiveRefreshingProvider::new(Arc::new(mock_provider.clone()));
        let res = provider.credentials().await;
        res.unwrap_err();
        let expire_at_1 = Utc::now() + chrono::Duration::seconds(2);
        mock_provider.set_credentials(Some(new_mock_credential(expire_at_1)));
        let res = provider.credentials().await;
        res.unwrap();
        let state = provider.state.lock().await;
        assert!(state.refresh_at.is_some());
        let refresh_at = state.refresh_at.unwrap();
        drop(state);
        assert!(refresh_at < expire_at_1);

        let expire_at_2 = Utc::now() + chrono::Duration::seconds(3);
        mock_provider.set_credentials(Some(new_mock_credential(expire_at_2)));
        // Before refresh_at, we still get the original credential.
        let res = provider.credentials().await;
        let creds = res.unwrap();
        assert_eq!(creds.expires_at().as_ref().unwrap().clone(), expire_at_1);

        tokio::time::sleep(Duration::from_millis(1200)).await;

        // When after refresh at, we get the latest credential.
        let expire_at_3 = Utc::now() + chrono::Duration::seconds(2);
        mock_provider.set_credentials(Some(new_mock_credential(expire_at_3)));
        let res = provider.credentials().await;
        let creds = res.unwrap();
        assert_eq!(creds.expires_at().as_ref().unwrap().clone(), expire_at_3);

        tokio::time::sleep(Duration::from_millis(1200)).await;
        mock_provider.set_credentials(None);

        // After refresh at, we failed to refresh the credential, still use the old one.
        let creds = provider.credentials().await.unwrap();
        assert_eq!(creds.expires_at().as_ref().unwrap().clone(), expire_at_3);

        tokio::time::sleep(Duration::from_millis(1000)).await;

        // After expire, and the provider is still unavailable, we get the error.
        let res = provider.credentials().await;
        res.unwrap_err();

        // When inner provider is available, we get the latest credential.
        let expire_at_4 = Utc::now() + chrono::Duration::seconds(2);
        mock_provider.set_credentials(Some(new_mock_credential(expire_at_4)));
        let creds = provider.credentials().await.unwrap();
        assert_eq!(*creds.expires_at().as_ref().unwrap(), expire_at_4);
    }
}

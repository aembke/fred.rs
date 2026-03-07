//! A [CredentialProvider](crate::types::config::CredentialProvider) implementation for AWS
//! ElastiCache IAM authentication.
//!
//! Generates short-lived pre-signed authentication tokens using SigV4, compatible with
//! ElastiCache Serverless `AUTH` commands. Tokens are valid for 15 minutes and the provider
//! refreshes them every 14 minutes by default.
//!
//! # Example
//!
//! ```rust,no_run
//! use fred::prelude::*;
//! use fred::credential_providers::elasticache::ElastiCacheCredentialProvider;
//! use std::sync::Arc;
//!
//! async fn example() -> Result<(), Error> {
//!   let provider = ElastiCacheCredentialProvider::new("my-cache", "my-user-id");
//!
//!   // Optionally assume an IAM role before signing:
//!   // let provider = provider.with_role_arn("arn:aws:iam::123456789012:role/my-role");
//!
//!   let mut config = Config::default();
//!   config.credential_provider = Some(Arc::new(provider));
//!
//!   let client = Client::new(config, None, None, None);
//!   client.init().await?;
//!   Ok(())
//! }
//! ```

use crate::{
  error::{Error, ErrorKind},
  types::config::Server,
};
use aws_credential_types::provider::ProvideCredentials;
use aws_sigv4::{
  http_request::{sign, SignableBody, SignableRequest, SignatureLocation, SigningSettings},
  sign::v4,
};
use std::{
  fmt,
  time::{Duration, SystemTime},
};

/// A [CredentialProvider](crate::types::config::CredentialProvider) for AWS ElastiCache IAM
/// authentication.
///
/// Uses AWS SigV4 to generate short-lived pre-signed tokens for ElastiCache Serverless,
/// passed as the password in Redis `AUTH` commands. AWS credentials are loaded from the
/// environment via the standard AWS SDK credential chain (environment variables, instance
/// metadata, ECS task role, etc.).
///
/// Tokens are valid for 15 minutes; [refresh_interval](Self::refresh_interval) returns 14
/// minutes to ensure they are rotated before expiry.
#[derive(Clone)]
pub struct ElastiCacheCredentialProvider {
  /// The ElastiCache serverless cache name used in the signing URL.
  cache_name:   String,
  /// The ElastiCache user ID to authenticate as.
  user_id:      String,
  /// An optional IAM role ARN to assume before generating the token.
  role_arn:     Option<String>,
  /// The session name used when assuming a role.
  session_name: String,
}

impl fmt::Debug for ElastiCacheCredentialProvider {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("ElastiCacheCredentialProvider")
      .field("cache_name", &self.cache_name)
      .field("user_id", &self.user_id)
      .field("role_arn", &self.role_arn)
      .field("session_name", &self.session_name)
      .finish()
  }
}

impl ElastiCacheCredentialProvider {
  /// Create a new provider for the given ElastiCache serverless cache name and user ID.
  ///
  /// `cache_name` is the serverless cache name used to construct the signing URL (e.g.
  /// `"my-cache"`). `user_id` is the ElastiCache user ID passed in the `AUTH` command.
  pub fn new(cache_name: impl Into<String>, user_id: impl Into<String>) -> Self {
    Self {
      cache_name:   cache_name.into(),
      user_id:      user_id.into(),
      role_arn:     None,
      session_name: "fred-elasticache".to_string(),
    }
  }

  /// Configure an IAM role ARN to assume before generating the authentication token.
  ///
  /// When set, the provider will call STS `AssumeRole` on each token refresh using the
  /// credentials available in the environment.
  pub fn with_role_arn(mut self, role_arn: impl Into<String>) -> Self {
    self.role_arn = Some(role_arn.into());
    self
  }

  /// Override the STS session name used when assuming the IAM role.
  ///
  /// Defaults to `"fred-elasticache"`. Has no effect if no role ARN is configured.
  pub fn with_session_name(mut self, session_name: impl Into<String>) -> Self {
    self.session_name = session_name.into();
    self
  }
}

#[async_trait::async_trait]
#[cfg(not(feature = "glommio"))]
impl crate::types::config::CredentialProvider for ElastiCacheCredentialProvider {
  async fn fetch(&self, _server: Option<&Server>) -> Result<(Option<String>, Option<String>), Error> {
    fetch_token(&self.cache_name, &self.user_id, self.role_arn.as_deref(), &self.session_name).await
  }

  fn refresh_interval(&self) -> Option<Duration> {
    // Tokens are valid for 15 minutes; refresh slightly before expiry.
    Some(Duration::from_secs(14 * 60))
  }
}

#[async_trait::async_trait(?Send)]
#[cfg(feature = "glommio")]
impl crate::types::config::CredentialProvider for ElastiCacheCredentialProvider {
  async fn fetch(&self, _server: Option<&Server>) -> Result<(Option<String>, Option<String>), Error> {
    fetch_token(&self.cache_name, &self.user_id, self.role_arn.as_deref(), &self.session_name).await
  }

  fn refresh_interval(&self) -> Option<Duration> {
    Some(Duration::from_secs(14 * 60))
  }
}

/// Build a pre-signed ElastiCache IAM authentication token.
///
/// The token is the authority + path/query of the signed URL, which ElastiCache accepts as
/// the password in a Redis `AUTH` command.
async fn fetch_token(
  cache_name: &str,
  user_id: &str,
  role_arn: Option<&str>,
  session_name: &str,
) -> Result<(Option<String>, Option<String>), Error> {
  let sdk_config = if let Some(role) = role_arn {
    _debug!("Assuming the role {role} for ElastiCache IAM authorization");
    let credentials_provider = aws_config::sts::AssumeRoleProvider::builder(role)
      .session_name(session_name)
      .build()
      .await;

    aws_config::from_env()
      .credentials_provider(credentials_provider)
      .load()
      .await
  } else {
    aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await
  };

  _trace!("Configuring ElastiCacheCredentialProvider with AWS sdk_config: {sdk_config:?}");

  let identity = sdk_config
    .credentials_provider()
    .ok_or_else(|| Error::new(ErrorKind::Auth, "no AWS credentials provider found in environment"))?
    .provide_credentials()
    .await
    .map_err(|e| Error::new(ErrorKind::Auth, format!("failed to load AWS credentials: {e:?}")))?
    .into();

  let region = sdk_config
    .region()
    .ok_or_else(|| Error::new(ErrorKind::Auth, "no AWS region configured"))?;

  let mut settings = SigningSettings::default();
  settings.expires_in = Some(Duration::from_secs(15 * 60));
  settings.signature_location = SignatureLocation::QueryParams;

  let params = v4::SigningParams::builder()
    .name("elasticache")
    .identity(&identity)
    .region(region.as_ref())
    .time(SystemTime::now())
    .settings(settings)
    .build()
    .map_err(|e| Error::new(ErrorKind::Auth, format!("failed to build SigV4 signing params: {e:?}")))?
    .into();

  let url = format!("http://{cache_name}?Action=connect&ResourceType=ServerlessCache&User={user_id}");
  _debug!("Url for Elasticache IAM authentication: {url}");

  let signable = SignableRequest::new("GET", &url, std::iter::empty(), SignableBody::Bytes(&[]))
    .map_err(|e| Error::new(ErrorKind::Auth, format!("failed to create signable request: {e:?}")))?;

  let (instructions, _) = sign(signable, &params)
    .map_err(|e| Error::new(ErrorKind::Auth, format!("failed to sign ElastiCache request: {e:?}")))?
    .into_parts();

  let mut req = http::Request::builder()
    .uri(&url)
    .body(())
    .map_err(|e| Error::new(ErrorKind::Auth, format!("failed to build HTTP request: {e:?}")))?;

  instructions.apply_to_request_http1x(&mut req);

  let parts = req.uri().clone().into_parts();
  let authority = parts
    .authority
    .ok_or_else(|| Error::new(ErrorKind::Auth, "missing URI authority after signing"))?;
  let path_and_query = parts
    .path_and_query
    .ok_or_else(|| Error::new(ErrorKind::Auth, "missing URI path/query after signing"))?;

  let token = format!("{authority}{path_and_query}");
  Ok((Some(user_id.to_string()), Some(token)))
}

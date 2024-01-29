#![allow(clippy::disallowed_names)]
#![allow(clippy::let_underscore_future)]

use async_trait::async_trait;
use bytes_utils::Str;
use fred::{prelude::*, types::Resolve};
use std::{net::SocketAddr, sync::Arc};
use trust_dns_resolver::{
  config::{ResolverConfig, ResolverOpts},
  TokioAsyncResolver,
};

pub struct TrustDnsResolver(TokioAsyncResolver);

impl TrustDnsResolver {
  fn new() -> Self {
    TrustDnsResolver(TokioAsyncResolver::tokio(
      ResolverConfig::default(),
      ResolverOpts::default(),
    ))
  }
}

#[async_trait]
impl Resolve for TrustDnsResolver {
  async fn resolve(&self, host: Str, port: u16) -> Result<Vec<SocketAddr>, RedisError> {
    Ok(
      self
        .0
        .lookup_ip(&*host)
        .await?
        .into_iter()
        .map(|ip| SocketAddr::new(ip, port))
        .collect(),
    )
  }
}

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let client = Builder::default_centralized().build()?;
  client.set_resolver(Arc::new(TrustDnsResolver::new())).await;
  client.init().await?;

  // ...

  client.quit().await?;
  Ok(())
}

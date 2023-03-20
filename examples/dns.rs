use async_trait::async_trait;
use fred::{prelude::*, types::Resolve};
use std::{net::SocketAddr, sync::Arc};
use trust_dns_resolver::{
  config::{ResolverConfig, ResolverOpts},
  TokioAsyncResolver,
};

pub struct TrustDnsResolver(TokioAsyncResolver);

impl TrustDnsResolver {
  fn new() -> Self {
    TrustDnsResolver(TokioAsyncResolver::tokio(ResolverConfig::default(), ResolverOpts::default()).unwrap())
  }
}

#[async_trait]
impl Resolve for TrustDnsResolver {
  async fn resolve(&self, host: String, port: u16) -> Result<SocketAddr, RedisError> {
    self.0.lookup_ip(&host).await.map_err(|e| e.into()).and_then(|ips| {
      let ip = match ips.iter().next() {
        Some(ip) => ip,
        None => return Err(RedisError::new(RedisErrorKind::IO, "Failed to lookup IP address.")),
      };

      Ok(SocketAddr::new(ip, port))
    })
  }
}

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let config = RedisConfig::default();
  let client = RedisClient::new(config, None, None);
  client.set_resolver(Arc::new(TrustDnsResolver::new())).await;

  let _ = client.connect();
  let _ = client.wait_for_connect().await?;

  // ...

  let _ = client.quit().await?;
  Ok(())
}

use crate::error::{RedisError, RedisErrorKind};
use crate::protocol::connection::RedisTransport;
use std::net::SocketAddr;
use tokio_rustls::TlsConnector;

pub async fn create_tls_transport(addr: SocketAddr) -> Result<RedisTransport, RedisError> {
  unimplemented!()
}

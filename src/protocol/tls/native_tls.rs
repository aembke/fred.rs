use crate::error::{RedisError, RedisErrorKind};
use crate::protocol::connection::RedisTransport;
use native_tls::TlsConnector as NativeTlsConnector;
use std::net::SocketAddr;

pub async fn create_tls_transport(addr: SocketAddr) -> Result<RedisTransport, RedisError> {
  unimplemented!()
}

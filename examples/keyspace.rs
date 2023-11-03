#![allow(clippy::disallowed_names)]
#![allow(clippy::let_underscore_future)]

use fred::{
  prelude::*,
  types::{RedisKey, Server},
};
use std::time::Duration;
use tokio::time::sleep;

async fn fake_traffic(client: &RedisClient, amount: usize) -> Result<(), RedisError> {
  for idx in 0 .. amount {
    let key: RedisKey = format!("foo-{}", idx).into();

    client.set(&key, 1, None, None, false).await?;
    client.incr(&key).await?;
    client.del(&key).await?;
  }

  Ok(())
}

/// Examples showing how to set up keyspace notifications with clustered or centralized/sentinel deployments.
///
/// The most complicated part of this process involves safely handling reconnections. Keyspace events rely on the
/// pubsub interface, and clients are required to subscribe or resubscribe whenever a new connection is created. These
/// examples show how to manually handle reconnections, but the caller can also use the `SubscriberClient` interface
/// to remove some of the boilerplate.
///
/// If callers do not need the keyspace subscriptions to survive reconnects then the process is more
/// straightforward.
#[tokio::main]
async fn main() -> Result<(), RedisError> {
  clustered_keyspace_events().await?;
  centralized_keyspace_events().await?;
  Ok(())
}

async fn centralized_keyspace_events() -> Result<(), RedisError> {
  let client = Builder::default_centralized().build()?;

  let reconnect_client = client.clone();
  // resubscribe to the foo- prefix whenever we reconnect to a server
  let reconnect_task = tokio::spawn(async move {
    let mut reconnect_rx = reconnect_client.reconnect_rx();

    while let Ok(server) = reconnect_rx.recv().await {
      println!("Reconnected to {}. Subscribing to keyspace events...", server);
      reconnect_client.psubscribe("__key__*:foo*").await?;
    }

    Ok::<_, RedisError>(())
  });

  // connect after setting up the reconnection logic
  client.connect();
  client.wait_for_connect().await?;

  // subscribe to almost all event types. probably don't copy this part.
  client.config_set("notify-keyspace-events", "AKE").await?;

  let keyspace_client = client.clone();
  // set up a task that listens for keyspace events
  let keyspace_task = tokio::spawn(async move {
    let mut keyspace_rx = keyspace_client.on_keyspace_event();

    while let Ok(event) = keyspace_rx.recv().await {
      println!(
        "Recv: {} on {} in db {}",
        event.operation,
        event.key.as_str_lossy(),
        event.db
      );
    }

    Ok::<_, RedisError>(())
  });

  // generate fake traffic and wait a second
  fake_traffic(&client, 1_000).await?;
  sleep(Duration::from_secs(1)).await;
  client.quit().await?;
  keyspace_task.await??;
  reconnect_task.await??;

  Ok(())
}

/// There are two approaches that can be used to inspect the cluster connections. One approach uses the cached cluster
/// state from `CLUSTER SLOTS` and the other uses the inner TCP connection map state. There are some trade-offs to
/// consider with each approach.
///
/// * The cached cluster state approach is *not* async but does require holding a mutex long enough to clone a
///   `ClusterRouting` struct. It may be out of date or in the process of changing, but callers can use
///   `on_cluster_change` to manually refresh their copy.
/// * The active connections approach is async and requires waiting for other commands or reconnection attempts to
///   finish first. It essentially acts like a user command to avoid any synchronization issues or race conditions,
///   but has the same drawbacks. However, this approach offers the most up-to-date view into the inner connection
///   state.
///
/// This example shows both approaches.
async fn read_servers(client: &RedisClient) -> Result<Vec<Server>, RedisError> {
  if rand::random::<u32>() % 2 == 0 {
    // use the cached cluster state
    client
      .cached_cluster_state()
      .map(|state| state.unique_primary_nodes())
      .ok_or(RedisError::new(
        RedisErrorKind::Cluster,
        "Missing cached cluster state.",
      ))
  } else {
    // use the active connections map
    client.active_connections().await
  }
}

async fn clustered_keyspace_events() -> Result<(), RedisError> {
  let client = Builder::default_clustered().build()?;

  let reconnect_client = client.clone();
  // resubscribe to the foo- prefix whenever we reconnect to a server
  let reconnect_task = tokio::spawn(async move {
    let mut reconnect_rx = reconnect_client.reconnect_rx();

    // in 7.x the reconnection interface added a `Server` struct to reconnect events to make this easier.
    while let Ok(server) = reconnect_rx.recv().await {
      println!("Reconnected to {}. Subscribing to keyspace events...", server);
      reconnect_client
        .with_cluster_node(server)
        .psubscribe("__key__*:foo*")
        .await?;
    }

    Ok::<_, RedisError>(())
  });

  // connect after setting up the reconnection logic
  client.connect();
  client.wait_for_connect().await?;

  let servers = read_servers(&client).await?;
  // subscribe to almost all event types on each cluster node.
  //
  // this could also go in the `on_reconnect` block, but shows an alternative approach that can be used instead if the
  // subscriptions do not need to survive reconnection events.
  for server in servers.into_iter() {
    // probably don't copy this part.
    client
      .with_cluster_node(server)
      .config_set("notify-keyspace-events", "AKE")
      .await?;
  }

  let keyspace_client = client.clone();
  // set up a task that listens for keyspace events
  let keyspace_task = tokio::spawn(async move {
    let mut keyspace_rx = keyspace_client.on_keyspace_event();

    while let Ok(event) = keyspace_rx.recv().await {
      println!(
        "Recv: {} on {} in db {}",
        event.operation,
        event.key.as_str_lossy(),
        event.db
      );
    }

    Ok::<_, RedisError>(())
  });

  // generate fake traffic and wait a second
  fake_traffic(&client, 1_000).await?;
  sleep(Duration::from_secs(1)).await;
  client.quit().await?;
  keyspace_task.await??;
  reconnect_task.await??;

  Ok(())
}

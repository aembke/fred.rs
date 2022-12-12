use fred::prelude::*;
use prometheus::{register_int_gauge_vec, IntGaugeVec};

fn sample_metrics(
  client: &RedisClient,
  num_commands: IntGaugeVec,
  avg_latency: IntGaugeVec,
  bytes_sent: IntGaugeVec,
) {
  let client_id = client.id().as_str();
  let latency_stats = client.take_latency_metrics();
  let req_size_stats = client.take_req_size_metrics();

  if let Ok(metric) = num_commands.get_metric_with_label_values(&[client_id]) {
    metric.set(latency_stats.samples as i64);
  }
  if let Ok(metric) = avg_latency.get_metric_with_label_values(&[client_id]) {
    metric.set(latency_stats.avg as i64);
  }
  if let Ok(metric) = bytes_sent.get_metric_with_label_values(&[client_id]) {
    metric.set(req_size_stats.sum as i64);
  }
}

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  // or use a counter here if you use `read_latency_metrics` instead of `take_latency_metrics` above, etc
  let num_commands = register_int_gauge_vec!("redis_num_commands", "Number of redis commands", &["id"]).unwrap();
  let avg_latency = register_int_gauge_vec!("redis_avg_latency", "Average latency to redis.", &["id"]).unwrap();
  let bytes_sent = register_int_gauge_vec!("redis_bytes_sent", "Total bytes sent to redis.", &["id"]).unwrap();

  let config = RedisConfig::default();
  let client = RedisClient::new(config, None, None);

  let _ = client.connect();
  let _ = client.wait_for_connect();

  // do stuff...

  sample_metrics(&client, num_commands, avg_latency, bytes_sent);
  let _ = client.quit().await?;
  Ok(())
}

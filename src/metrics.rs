use std::cmp;

/// Stats describing a distribution of samples.
pub struct DistributionStats {
  pub min: u64,
  pub max: u64,
  pub avg: f64,
  pub stddev: f64,
  pub samples: u64,
  pub sum: u64,
}

/// Stats describing latency metrics for requests.
pub struct LatencyStats {
  pub min: i64,
  pub max: i64,
  pub avg: f64,
  pub variance: f64,
  pub samples: usize,
  pub sum: u64,
  old_avg: f64,
  s: f64,
  old_s: f64,
}

impl Default for LatencyStats {
  fn default() -> Self {
    LatencyStats {
      min: 0,
      max: 0,
      avg: 0.0,
      sum: 0,
      variance: 0.0,
      samples: 0,
      s: 0.0,
      old_s: 0.0,
      old_avg: 0.0,
    }
  }
}

impl LatencyStats {
  pub fn sample(&mut self, latency: i64) {
    self.samples += 1;
    let latency_count = self.samples as f64;
    let latency_f = latency as f64;
    self.sum += cmp::max(0, latency) as u64;

    if self.samples == 1 {
      self.avg = latency_f;
      self.variance = 0.0;
      self.old_avg = latency_f;
      self.old_s = 0.0;
      self.min = latency;
      self.max = latency;
    } else {
      self.avg = self.old_avg + (latency_f - self.old_avg) / latency_count;
      self.s = self.old_s + (latency_f - self.old_avg) * (latency_f - self.avg);

      self.old_avg = self.avg;
      self.old_s = self.s;
      self.variance = self.s / (latency_count - 1.0);

      self.min = cmp::min(self.min, latency);
      self.max = cmp::max(self.max, latency);
    }
  }

  pub fn reset(&mut self) {
    self.min = 0;
    self.max = 0;
    self.avg = 0.0;
    self.variance = 0.0;
    self.samples = 0;
    self.sum = 0;
    self.s = 0.0;
    self.old_s = 0.0;
    self.old_avg = 0.0;
  }

  pub fn read_metrics(&self) -> DistributionStats {
    self.into()
  }

  pub fn take_metrics(&mut self) -> DistributionStats {
    let metrics = self.read_metrics();
    self.reset();
    metrics
  }
}

impl<'a> From<&'a LatencyStats> for DistributionStats {
  fn from(stats: &'a LatencyStats) -> DistributionStats {
    DistributionStats {
      avg: stats.avg,
      stddev: stats.variance.sqrt(),
      min: cmp::max(stats.min, 0) as u64,
      max: cmp::max(stats.max, 0) as u64,
      samples: stats.samples as u64,
      sum: stats.sum,
    }
  }
}

/// Stats describing sizes (in bytes) for requests or responses.
pub struct SizeStats {
  pub min: u64,
  pub max: u64,
  pub avg: f64,
  pub variance: f64,
  pub samples: usize,
  pub sum: u64,
  old_avg: f64,
  s: f64,
  old_s: f64,
}

impl Default for SizeStats {
  fn default() -> Self {
    SizeStats {
      min: 0,
      max: 0,
      avg: 0.0,
      variance: 0.0,
      samples: 0,
      sum: 0,
      s: 0.0,
      old_s: 0.0,
      old_avg: 0.0,
    }
  }
}

impl SizeStats {
  pub fn sample(&mut self, size: u64) {
    self.samples = self.samples.saturating_add(1);
    let size_count = self.samples as f64;
    let size_f = size as f64;
    self.sum = self.sum.saturating_add(size);

    if self.samples == 1 {
      self.avg = size_f;
      self.variance = 0.0;
      self.old_avg = size_f;
      self.old_s = 0.0;
      self.min = size;
      self.max = size;
    } else {
      self.avg = self.old_avg + (size_f - self.old_avg) / size_count;
      self.s = self.old_s + (size_f - self.old_avg) * (size_f - self.avg);

      self.old_avg = self.avg;
      self.old_s = self.s;
      self.variance = self.s / (size_count - 1.0);

      self.min = cmp::min(self.min, size);
      self.max = cmp::max(self.max, size);
    }
  }

  pub fn reset(&mut self) {
    self.min = 0;
    self.max = 0;
    self.avg = 0.0;
    self.variance = 0.0;
    self.samples = 0;
    self.sum = 0;
    self.s = 0.0;
    self.old_s = 0.0;
    self.old_avg = 0.0;
  }

  pub fn read_metrics(&self) -> DistributionStats {
    self.into()
  }

  pub fn take_metrics(&mut self) -> DistributionStats {
    let metrics = self.read_metrics();
    self.reset();
    metrics
  }
}

impl<'a> From<&'a SizeStats> for DistributionStats {
  fn from(stats: &'a SizeStats) -> DistributionStats {
    DistributionStats {
      avg: stats.avg,
      stddev: stats.variance.sqrt(),
      min: stats.min,
      max: stats.max,
      samples: stats.samples as u64,
      sum: stats.sum,
    }
  }
}

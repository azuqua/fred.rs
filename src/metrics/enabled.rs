
use chrono::prelude::*;

use std::sync::Arc;
use parking_lot::RwLock;
use std::ops::{
  Deref,
  DerefMut
};

use std::cmp::{min, max};

#[doc(hidden)]
pub fn now_utc_ms() -> Option<i64> {
  let time = Utc::now();
  Some(time.timestamp() * 1000 + (time.timestamp_subsec_millis() as i64))
}

#[doc(hidden)]
pub fn sample_latency(tracker: &Arc<RwLock<LatencyTracker>>, latency: i64) {
  let mut tracker_guard = tracker.write();
  tracker_guard.deref_mut().sample(latency);
}

#[doc(hidden)]
pub fn sample_size(tracker: &Arc<RwLock<SizeTracker>>, size: u64) {
  let mut tracker_guard = tracker.write();
  tracker_guard.deref_mut().sample(size);
}

#[doc(hidden)]
pub fn read_latency_stats(tracker: &Arc<RwLock<LatencyTracker>>) -> LatencyMetrics {
  let tracker_guard = tracker.read();
  tracker_guard.deref().read_metrics()
}

#[doc(hidden)]
pub fn read_size_stats(tracker: &Arc<RwLock<SizeTracker>>) -> SizeMetrics {
  let tracker_guard = tracker.read();
  tracker_guard.deref().read_metrics()
}

#[doc(hidden)]
pub fn take_latency_stats(tracker: &Arc<RwLock<LatencyTracker>>) -> LatencyMetrics {
  let mut tracker_guard = tracker.write();
  tracker_guard.deref_mut().take_metrics()
}

#[doc(hidden)]
pub fn take_size_stats(tracker: &Arc<RwLock<SizeTracker>>) -> SizeMetrics {
  let mut tracker_guard = tracker.write();
  tracker_guard.deref_mut().take_metrics()
}

/// Latency metrics across all Redis commands, measured in milliseconds.
#[derive(Clone, Debug)]
pub struct LatencyMetrics {
  pub min: i64,
  pub max: i64,
  pub avg: f64,
  pub stddev: f64,
  pub samples: usize
}

/// Payload size metrics across all Redis commands, measured in bytes.
#[derive(Clone, Debug)]
pub struct SizeMetrics {
  pub min: u64,
  pub max: u64,
  pub avg: f64,
  pub stddev: f64,
  pub samples: usize
}

#[doc(hidden)]
pub struct LatencyTracker {
  pub min: i64,
  pub max: i64,
  pub avg: f64,
  pub variance: f64,
  pub samples: usize,
  old_avg: f64,
  s: f64,
  old_s: f64
}

impl Default for LatencyTracker {
  fn default() -> Self {
    LatencyTracker {
      min: 0,
      max: 0,
      avg: 0.0,
      variance: 0.0,
      samples: 0,
      s: 0.0,
      old_s: 0.0,
      old_avg: 0.0
    }
  }
}

impl LatencyTracker {

  pub fn sample(&mut self, latency: i64) {
    self.samples += 1;
    let latency_count = self.samples as f64;
    let latency_f = latency as f64;

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
      self.variance = self.s/(latency_count - 1.0);

      self.min = min(self.min, latency);
      self.max = max(self.max, latency);
    }
  }

  pub fn reset(&mut self) {
    self.min = 0;
    self.max = 0;
    self.avg = 0.0;
    self.variance = 0.0;
    self.samples = 0;
    self.s = 0.0;
    self.old_s = 0.0;
    self.old_avg = 0.0;
  }

  pub fn read_metrics(&self) -> LatencyMetrics {
    LatencyMetrics {
      min: self.min,
      max: self.max,
      avg: self.avg,
      stddev: self.variance.sqrt(),
      samples: self.samples
    }
  }

  pub fn take_metrics(&mut self) -> LatencyMetrics {
    let metrics = self.read_metrics();
    self.reset();
    metrics
  }

}

#[doc(hidden)]
pub struct SizeTracker {
  pub min: u64,
  pub max: u64,
  pub avg: f64,
  pub variance: f64,
  pub samples: usize,
  old_avg: f64,
  s: f64,
  old_s: f64
}

impl Default for SizeTracker {
  fn default() -> Self {
    SizeTracker {
      min: 0,
      max: 0,
      avg: 0.0,
      variance: 0.0,
      samples: 0,
      s: 0.0,
      old_s: 0.0,
      old_avg: 0.0
    }
  }
}

impl SizeTracker {

  pub fn sample(&mut self, size: u64) {
    self.samples += 1;
    let size_count = self.samples as f64;
    let size_f = size as f64;

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
      self.variance = self.s/(size_count - 1.0);

      self.min = min(self.min, size);
      self.max = max(self.max, size);
    }
  }

  pub fn reset(&mut self) {
    self.min = 0;
    self.max = 0;
    self.avg = 0.0;
    self.variance = 0.0;
    self.samples = 0;
    self.s = 0.0;
    self.old_s = 0.0;
    self.old_avg = 0.0;
  }

  pub fn read_metrics(&self) -> SizeMetrics {
    SizeMetrics {
      min: self.min,
      max: self.max,
      avg: self.avg,
      stddev: self.variance.sqrt(),
      samples: self.samples
    }
  }

  pub fn take_metrics(&mut self) -> SizeMetrics {
    let metrics = self.read_metrics();
    self.reset();
    metrics
  }

}

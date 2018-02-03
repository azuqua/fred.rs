
use std::sync::Arc;
use parking_lot::RwLock;


#[doc(hidden)]
pub fn now_utc_ms() -> Option<i64> {
  None
}

#[doc(hidden)]
pub fn sample_latency(_tracker: &Arc<RwLock<LatencyTracker>>, _latency: i64) {}

#[doc(hidden)]
pub fn sample_size(_tracker: &Arc<RwLock<SizeTracker>>, _size: u64) {}

#[doc(hidden)]
pub struct LatencyTracker {}

impl Default for LatencyTracker {
  fn default() -> Self {
    LatencyTracker {}
  }
}

impl LatencyTracker {

  pub fn sample(&mut self, _latency: i64) {}

  pub fn reset(&mut self) {}

}

#[doc(hidden)]
pub struct SizeTracker {}

impl Default for SizeTracker {
  fn default() -> Self {
    SizeTracker {}
  }
}

impl SizeTracker {

  pub fn sample(&mut self, _size: u64) {}

  pub fn reset(&mut self) {}

}

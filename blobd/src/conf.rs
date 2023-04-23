use serde::Deserialize;
use serde::Serialize;
use std::net::Ipv4Addr;
use std::path::PathBuf;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Conf {
  // Only used when formatting.
  // Minimum 4096, maximum 281474976710656.
  pub bucket_count: u64,
  #[serde(default = "default_reap_objects_after_secs")]
  pub reap_objects_after_secs: u64,
  #[serde(default)]
  pub versioning: bool,

  pub device_path: PathBuf,

  // This is required even if authentication is disabled, as it's used for other internal features too.
  pub token_secret_base64: String,
  #[serde(default)]
  pub disable_authentication: bool,

  pub interface: Ipv4Addr,
  pub port: u16,

  // Advanced configuration.
  #[serde(default = "default_bucket_lock_count_log2")]
  pub bucket_lock_count_log2: u8,
  #[serde(default = "default_lpage_size_pow2")]
  pub lpage_size_pow2: u8,
  #[serde(default = "default_spage_size_pow2")]
  pub spage_size_pow2: u8,
}

const fn default_reap_objects_after_secs() -> u64 {
  604800
}
const fn default_bucket_lock_count_log2() -> u8 {
  24
}
const fn default_lpage_size_pow2() -> u8 {
  24
}
const fn default_spage_size_pow2() -> u8 {
  9
}

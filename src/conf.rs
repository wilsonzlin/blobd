use serde::Deserialize;
use serde::Serialize;
use std::net::Ipv4Addr;
use std::path::PathBuf;

#[derive(Serialize, Deserialize)]
pub struct Conf {
  // Only used when formatting.
  // Minimum 4096, maximum 281474976710656.
  pub bucket_count: u64,

  pub device_path: PathBuf,

  pub token_secret_base64: String,

  pub interface: Ipv4Addr,
  pub port: u16,
}

use std::{path::PathBuf, net::Ipv4Addr};

use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
pub struct Conf {
  // Only used when formatting.
  // Minimum 4096, maximum 281474976710656.
  pub bucket_count: u64,

  pub device_path: PathBuf,

  pub interface: Ipv4Addr,
  pub port: u16,
}

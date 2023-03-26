use std::path::PathBuf;

pub struct Conf {
  // Only used when formatting.
  // Minimum 4096, maximum 281474976710656.
  pub bucket_count: u64,

  pub device_path: PathBuf,

  pub worker_address: PathBuf,
  pub worker_port: u16,
  pub worker_threads: u16,
  pub worker_unix_socket_path: PathBuf,
}

use axum::http::Uri;
use itertools::Itertools;
use percent_encoding::percent_decode;

pub mod create_object;
pub mod write_object;
pub mod read_object;
pub mod delete_object;
pub mod commit_object;

pub fn parse_key(uri: &Uri) -> (Vec<u8>, u16) {
  let key = percent_decode(uri.path().as_bytes()).collect_vec();
  let key_len: u16 = key.len().try_into().unwrap();
  (key, key_len)
}

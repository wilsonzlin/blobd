use crate::token::BlobdTokens;
use axum::http::Uri;
use data_encoding::BASE64;
use itertools::Itertools;
use off64::Off64Int;
use percent_encoding::percent_decode;
use rand::thread_rng;
use rand::RngCore;
use rmp_serde::Serializer;
use serde::Deserialize;
use serde::Serialize;

pub mod commit_object;
pub mod create_object;
pub mod delete_object;
pub mod inspect_object;
pub mod read_object;
pub mod write_object;

pub fn parse_key(uri: &Uri) -> (Vec<u8>, u16) {
  let key = percent_decode(uri.path().strip_prefix("/").unwrap().as_bytes()).collect_vec();
  let key_len: u16 = key.len().try_into().unwrap();
  (key, key_len)
}

// (inode_device_offset, salt, hmac)
// We don't respond with or require the inode device offset directly, as an incorrect value (unintentional or otherwise) could cause corruption.
#[derive(Serialize, Deserialize)]
pub struct UploadId {
  // WARNING: Order of fields is significant, as rmp_serde will serialise in this order without field names.
  inode_dev_offset: u64,
  salt: [u8; 24],
  mac: Vec<u8>,
}

impl UploadId {
  fn generate_mac_data(inode_dev_offset: u64, salt: &[u8; 24]) -> Vec<u8> {
    let mut data = vec![0u8; 32];
    data.write_u64_be_at(0, inode_dev_offset);
    data[8..].copy_from_slice(salt);
    data
  }

  pub fn new(tokens: &BlobdTokens, inode_dev_offset: u64) -> String {
    let mut salt = [0u8; 24];
    thread_rng().fill_bytes(&mut salt);
    let data = Self::generate_mac_data(inode_dev_offset, &salt);
    let mac = tokens.generate(&data);

    let id = UploadId {
      inode_dev_offset,
      mac,
      salt,
    };
    let mut token_raw = Vec::new();
    id.serialize(&mut Serializer::new(&mut token_raw)).unwrap();
    BASE64.encode(&token_raw)
  }

  pub fn parse_and_verify(tokens: &BlobdTokens, token: &str) -> Option<u64> {
    let token_raw = BASE64.decode(token.as_bytes()).ok()?;
    let id: UploadId = rmp_serde::decode::from_slice(&token_raw).ok()?;

    let data = Self::generate_mac_data(id.inode_dev_offset, &id.salt);
    if !tokens.verify(&data, &id.mac) {
      return None;
    };
    Some(id.inode_dev_offset)
  }
}

use crate::token::BlobdTokens;
use axum::http::Uri;
use chrono::DateTime;
use chrono::Utc;
use data_encoding::BASE64;
use data_encoding::BASE64URL_NOPAD;
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

// TODO Deny %2F (if slash is intentional, provide it directly; if not, it will be mixed with literal slashes once decoded).
// TODO Deny empty string.
pub fn parse_key(uri: &Uri) -> (Vec<u8>, u16) {
  let key = percent_decode(uri.path().strip_prefix("/").unwrap().as_bytes()).collect_vec();
  let key_len: u16 = key.len().try_into().unwrap();
  (key, key_len)
}

// (inode_device_offset, salt, hmac)
// We don't respond with or require the inode device offset directly, as an incorrect value (unintentional or otherwise) could cause corruption.
#[derive(Serialize, Deserialize)]
// WARNING: Order of fields is significant, as rmp_serde will serialise in this order without field names.
pub struct UploadId {
  inode_dev_offset: u64,
  salt: [u8; 16],
  mac: Vec<u8>,
}

impl UploadId {
  fn generate_mac_data(inode_dev_offset: u64, salt: &[u8; 16]) -> Vec<u8> {
    let mut data = vec![0u8; 32];
    data.write_u64_be_at(0, inode_dev_offset);
    data[8..].copy_from_slice(salt);
    data
  }

  pub fn new(tokens: &BlobdTokens, inode_dev_offset: u64) -> String {
    let mut salt = [0u8; 16];
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

#[derive(Serialize, Deserialize, PartialEq, Eq)]
// WARNING: Order of fields is significant, as rmp_serde will serialise in this order without field names.
pub enum AuthTokenAction {
  CommitObject { object_id: u64 },
  CreateObject { key: Vec<u8>, size: u64 },
  DeleteObject { key: Vec<u8> },
  InspectObject { key: Vec<u8> },
  ReadObject { key: Vec<u8> },
  WriteObject { object_id: u64, offset: u64 },
}

#[derive(Serialize, Deserialize)]
// WARNING: Order of fields is significant, as rmp_serde will serialise in this order without field names.
pub struct AuthToken {
  action: AuthTokenAction,
  expires: i64, // UNIX timestamp, seconds since epoch.
  salt: [u8; 16],
  mac: Vec<u8>,
}

impl AuthToken {
  fn generate_mac_data(action: &AuthTokenAction, expires: i64, salt: &[u8; 16]) -> Vec<u8> {
    let mut data = salt.to_vec();
    data.extend_from_slice(&expires.to_be_bytes());
    action.serialize(&mut Serializer::new(&mut data)).unwrap();
    data
  }

  pub fn new(tokens: &BlobdTokens, action: AuthTokenAction, expires: DateTime<Utc>) -> String {
    let expires = expires.timestamp();

    let mut salt = [0u8; 16];
    thread_rng().fill_bytes(&mut salt);
    let data = Self::generate_mac_data(&action, expires, &salt);
    let mac = tokens.generate(&data);

    let v = AuthToken {
      action,
      expires,
      salt,
      mac,
    };
    let mut token_raw = Vec::new();
    v.serialize(&mut Serializer::new(&mut token_raw)).unwrap();
    BASE64URL_NOPAD.encode(&token_raw)
  }

  pub fn verify(tokens: &BlobdTokens, token: &str, expected_action: AuthTokenAction) -> bool {
    let now = Utc::now().timestamp();
    let Ok(token_raw) = BASE64URL_NOPAD.decode(token.as_bytes()) else {
      return false;
    };
    let Some(v): Option<AuthToken> = rmp_serde::decode::from_slice(&token_raw).ok() else {
      return false;
    };

    let data = Self::generate_mac_data(&v.action, v.expires, &v.salt);
    tokens.verify(&data, &v.mac) && v.expires > now && v.action == expected_action
  }
}

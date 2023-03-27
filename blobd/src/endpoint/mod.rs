use axum::http::Uri;
use blobd_token::BlobdTokens;
use data_encoding::BASE64;
use itertools::Itertools;
use percent_encoding::percent_decode;
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
}

impl UploadId {
  pub fn new(tokens: &BlobdTokens, inode_dev_offset: u64) -> String {
    let id = UploadId { inode_dev_offset };
    let token_raw = tokens.generate(id);
    BASE64.encode(&token_raw)
  }

  pub fn parse_and_verify(tokens: &BlobdTokens, token: &str) -> Option<u64> {
    let token_raw = BASE64.decode(token.as_bytes()).ok()?;
    let id = tokens.parse_and_verify::<UploadId>(&token_raw)?;
    Some(id.inode_dev_offset)
  }
}

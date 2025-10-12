use crate::libblobd::incomplete_token::IncompleteToken;
use crate::libblobd::op::OpError;
use crate::libblobd::Blobd;
use axum::http::StatusCode;
use axum::http::Uri;
use blake3::Hash;
use blobd_token::AuthToken;
use blobd_token::AuthTokenAction;
use blobd_token::BlobdTokens;
use data_encoding::BASE64URL_NOPAD;
use off64::usz;
use percent_encoding::percent_decode;
use rmp_serde::Serializer;
use serde::Serialize;

pub mod batch_create_objects;
pub mod commit_object;
pub mod create_object;
pub mod delete_object;
pub mod inspect_object;
pub mod read_object;
pub mod write_object;

pub struct HttpCtx {
  pub authentication_is_enabled: bool,
  pub blobd: Blobd,
  pub tokens: BlobdTokens,
  pub token_secret: [u8; 32],
}

fn div_ceil(n: u64, d: u64) -> u64 {
  (n / d) + ((n % d != 0) as u64)
}

impl HttpCtx {
  pub fn verify_auth(&self, t: &str, expected: AuthTokenAction) -> bool {
    !self.authentication_is_enabled || AuthToken::verify(&self.tokens, t, expected)
  }

  // We don't respond with or require the raw incomplete token directly, as an incorrect value (unintentional or otherwise) could cause corruption.
  pub fn generate_upload_token(
    &self,
    incomplete_token: IncompleteToken,
    object_size: u64,
  ) -> String {
    let mut data = Vec::new();
    (incomplete_token, object_size)
      .serialize(&mut Serializer::new(&mut data))
      .unwrap();
    let mac = blake3::keyed_hash(&self.token_secret, &data);
    let mut raw = mac.as_bytes().to_vec();
    raw.extend_from_slice(&data);
    BASE64URL_NOPAD.encode(&raw)
  }

  // Returns (incomplete_token, object_size).
  pub fn parse_and_verify_upload_token(
    &self,
    upload_token: &str,
  ) -> Option<(IncompleteToken, u64)> {
    let raw = BASE64URL_NOPAD.decode(upload_token.as_bytes()).ok()?;
    if raw.len() <= 32 {
      return None;
    };
    let (mac, data_serialised) = raw.split_at(32);
    let mac: [u8; 32] = mac.try_into().unwrap();
    let mac: Hash = mac.into();
    if mac != blake3::keyed_hash(&self.token_secret, data_serialised) {
      return None;
    };
    let (incomplete_token, object_size): (IncompleteToken, u64) =
      rmp_serde::from_slice(data_serialised).ok()?;
    Some((incomplete_token, object_size))
  }

  fn generate_write_receipt_mac(&self, incomplete_token: IncompleteToken, part_idx: u64) -> Hash {
    let mut data = Vec::new();
    (incomplete_token, part_idx)
      .serialize(&mut Serializer::new(&mut data))
      .unwrap();
    blake3::keyed_hash(&self.token_secret, &data)
  }

  // This ensures all solid tiles and the tail data have been written to for an incomplete object before committing it, as otherwise we'd expose raw non-overwritten data that could contain sensitive/private data from deleted objects.
  pub fn generate_write_receipt(&self, incomplete_token: IncompleteToken, part_idx: u64) -> String {
    let raw = self.generate_write_receipt_mac(incomplete_token, part_idx);
    BASE64URL_NOPAD.encode(raw.as_bytes())
  }

  pub fn verify_write_receipts(
    &self,
    receipts: &[&str],
    incomplete_token: IncompleteToken,
    object_size: u64,
  ) -> Option<()> {
    let part_count = div_ceil(object_size, self.blobd.cfg().lpage_size());
    for part_idx in 0..part_count {
      let receipt = receipts.get(usz!(part_idx))?;
      let mac = BASE64URL_NOPAD.decode(receipt.as_bytes()).ok()?;
      let mac: [u8; 32] = mac.try_into().ok()?;
      let mac: Hash = mac.try_into().ok()?;
      if mac != self.generate_write_receipt_mac(incomplete_token, part_idx) {
        return None;
      };
    }
    Some(())
  }
}

pub fn transform_op_error(err: OpError) -> StatusCode {
  match err {
    OpError::DataStreamError(_) => StatusCode::REQUEST_TIMEOUT,
    OpError::DataStreamLengthMismatch => StatusCode::RANGE_NOT_SATISFIABLE,
    OpError::InexactWriteLength => StatusCode::RANGE_NOT_SATISFIABLE,
    OpError::ObjectMetadataTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
    OpError::ObjectNotFound => StatusCode::NOT_FOUND,
    OpError::RangeOutOfBounds => StatusCode::RANGE_NOT_SATISFIABLE,
    OpError::UnalignedWrite => StatusCode::RANGE_NOT_SATISFIABLE,
  }
}

// TODO Deny %2F (if slash is intentional, provide it directly; if not, it will be mixed with literal slashes once decoded).
// TODO Deny empty string.
pub fn parse_key(uri: &Uri) -> Vec<u8> {
  percent_decode(uri.path().strip_prefix("/").unwrap().as_bytes()).collect()
}

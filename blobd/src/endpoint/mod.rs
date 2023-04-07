use aes_gcm::aead::Aead;
use aes_gcm::Aes256Gcm;
use aes_gcm::Nonce;
use axum::http::StatusCode;
use axum::http::Uri;
use blobd_token::AuthToken;
use blobd_token::AuthTokenAction;
use blobd_token::BlobdTokens;
use data_encoding::BASE64URL_NOPAD;
use hmac::digest::CtOutput;
use hmac::digest::Output;
use hmac::Hmac;
use hmac::Mac;
use itertools::Itertools;
use libblobd::incomplete_slots::IncompleteSlotId;
use libblobd::op::OpError;
use libblobd::tile::TILE_SIZE_U64;
use libblobd::Blobd;
use off64::usz;
use off64::Off64Int;
use percent_encoding::percent_decode;
use sha2::Sha512;

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
  pub token_secret_aes_gcm: Aes256Gcm,
}

fn div_ceil(n: u64, d: u64) -> u64 {
  (n / d) + ((n % d != 0) as u64)
}

impl HttpCtx {
  pub fn verify_auth(&self, t: &str, expected: AuthTokenAction) -> bool {
    !self.authentication_is_enabled || AuthToken::verify(&self.tokens, t, expected)
  }

  // We don't respond with or require the slot ID directly, as an incorrect value (unintentional or otherwise) could cause corruption.
  // We need the object ID in order to use it as a nonce. It also ensures that the token cannot be reused across slots/objects accidentally.
  pub fn generate_upload_token(
    &self,
    object_id: u64,
    slot_id: IncompleteSlotId,
    object_size: u64,
  ) -> String {
    let part_count = div_ceil(object_size, TILE_SIZE_U64);
    let mut nonce = vec![0u8; 12];
    nonce.write_u64_be_at(0, object_id);
    let mut data = Vec::new();
    data.extend_from_slice(&slot_id.to_be_bytes());
    data.extend_from_slice(&part_count.to_be_bytes());
    let ciphertext = self
      .token_secret_aes_gcm
      .encrypt(Nonce::from_slice(&nonce), &*data)
      .unwrap();
    let mut raw = nonce;
    raw.extend_from_slice(&ciphertext);
    BASE64URL_NOPAD.encode(&raw)
  }

  // Returns (slot_id, part_count).
  pub fn parse_and_verify_upload_token(
    &self,
    expected_object_id: u64,
    upload_token: &str,
  ) -> Option<(IncompleteSlotId, u64)> {
    let raw = BASE64URL_NOPAD.decode(upload_token.as_bytes()).ok()?;
    if raw.len() <= 12 {
      return None;
    };
    let (nonce, ciphertext) = raw.split_at(12);
    let object_id = nonce.read_u64_be_at(0);
    if object_id != expected_object_id {
      return None;
    };
    let nonce = Nonce::from_slice(nonce);
    let bytes = self.token_secret_aes_gcm.decrypt(nonce, ciphertext).ok()?;
    let slot_id = bytes.read_u64_be_at(0);
    let part_count = bytes.read_u64_be_at(8);
    Some((slot_id, part_count))
  }

  fn generate_write_receipt_mac(
    &self,
    object_id: u64,
    slot_id: IncompleteSlotId,
    part_idx: u64,
  ) -> CtOutput<Hmac<Sha512>> {
    let mut mac = Hmac::<Sha512>::new_from_slice(&self.token_secret).unwrap();
    mac.update(&object_id.to_be_bytes());
    mac.update(&slot_id.to_be_bytes());
    mac.update(&part_idx.to_be_bytes());
    mac.finalize()
  }

  // This ensures all solid tiles and the tail data have been written to for an incomplete object before committing it, as otherwise we'd expose raw non-overwritten data that could contain sensitive/private data from deleted objects.
  pub fn generate_write_receipt(
    &self,
    object_id: u64,
    slot_id: IncompleteSlotId,
    part_idx: u64,
  ) -> String {
    let raw = self.generate_write_receipt_mac(object_id, slot_id, part_idx);
    BASE64URL_NOPAD.encode(&raw.into_bytes())
  }

  pub fn verify_write_receipts(
    &self,
    receipts: &[&str],
    expected_object_id: u64,
    expected_slot_id: IncompleteSlotId,
    part_count: u64,
  ) -> bool {
    for part_idx in 0..part_count {
      let Some(receipt) = receipts.get(usz!(part_idx)) else { return false; };
      let Ok(mac) = BASE64URL_NOPAD.decode(receipt.as_bytes()) else { return false; };
      let Some(mac): Option<[u8; 64]> = mac.try_into().ok() else { return false; };
      let Some(mac): Option<Output<Hmac<Sha512>>> = mac.try_into().ok() else { return false; };
      let mac = CtOutput::from(mac);
      if mac != self.generate_write_receipt_mac(expected_object_id, expected_slot_id, part_idx) {
        return false;
      };
    }
    true
  }
}

pub fn transform_op_error(err: OpError) -> StatusCode {
  match err {
    OpError::DataStreamError(_) => StatusCode::REQUEST_TIMEOUT,
    OpError::DataStreamLengthMismatch => StatusCode::RANGE_NOT_SATISFIABLE,
    OpError::InexactWriteLength => StatusCode::RANGE_NOT_SATISFIABLE,
    OpError::ObjectNotFound => StatusCode::NOT_FOUND,
    OpError::RangeOutOfBounds => StatusCode::RANGE_NOT_SATISFIABLE,
    OpError::UnalignedWrite => StatusCode::RANGE_NOT_SATISFIABLE,
  }
}

// TODO Deny %2F (if slash is intentional, provide it directly; if not, it will be mixed with literal slashes once decoded).
// TODO Deny empty string.
pub fn parse_key(uri: &Uri) -> Vec<u8> {
  percent_decode(uri.path().strip_prefix("/").unwrap().as_bytes()).collect_vec()
}

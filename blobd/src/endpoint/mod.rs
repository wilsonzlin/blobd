use aes_gcm::aead::Aead;
use aes_gcm::Aes256Gcm;
use aes_gcm::Nonce;
use axum::http::StatusCode;
use axum::http::Uri;
use blobd_token::AuthToken;
use blobd_token::AuthTokenAction;
use blobd_token::BlobdTokens;
use data_encoding::BASE64URL_NOPAD;
use itertools::Itertools;
use libblobd::incomplete_slots::IncompleteSlotId;
use libblobd::op::OpError;
use libblobd::Blobd;
use percent_encoding::percent_decode;
use rand::thread_rng;
use rand::RngCore;

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
  pub token_secret_aes_gcm: Aes256Gcm,
}

impl HttpCtx {
  pub fn verify_auth(&self, t: &str, expected: AuthTokenAction) -> bool {
    !self.authentication_is_enabled || AuthToken::verify(&self.tokens, t, expected)
  }

  // We don't respond with or require the slot ID directly, as an incorrect value (unintentional or otherwise) could cause corruption.
  pub fn generate_upload_id(&self, slot_id: IncompleteSlotId) -> String {
    let mut nonce = vec![0u8; 12];
    thread_rng().fill_bytes(&mut nonce);
    let ciphertext = self
      .token_secret_aes_gcm
      .encrypt(Nonce::from_slice(&nonce), slot_id.to_be_bytes().as_ref())
      .unwrap();
    let mut raw = nonce;
    raw.extend_from_slice(&ciphertext);
    BASE64URL_NOPAD.encode(&raw)
  }

  pub fn parse_and_verify_upload_id(&self, upload_id: &str) -> Option<IncompleteSlotId> {
    let raw = BASE64URL_NOPAD.decode(upload_id.as_bytes()).ok()?;
    if raw.len() <= 12 {
      return None;
    };
    let (nonce, ciphertext) = raw.split_at(12);
    let nonce = Nonce::from_slice(nonce);
    let bytes = self.token_secret_aes_gcm.decrypt(nonce, ciphertext).ok()?;
    Some(u64::from_be_bytes(bytes.try_into().unwrap()))
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

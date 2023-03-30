use data_encoding::BASE64URL_NOPAD;
use hmac::digest::CtOutput;
use hmac::digest::Output;
use hmac::Hmac;
use hmac::Mac;
use serde::Deserialize;
use serde::Serialize;
use sha2::Sha256;
use std::mem::size_of;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

pub struct BlobdTokens {
  secret: [u8; 32],
}

type BlobdTokenHmac = Hmac<Sha256>;

const BLOBD_TOKEN_HMAC_LEN: usize = size_of::<Output<BlobdTokenHmac>>();

impl BlobdTokens {
  pub fn new(secret: [u8; 32]) -> Self {
    Self { secret }
  }

  fn calculate_mac<T: Serialize>(&self, token_data: &T) -> CtOutput<BlobdTokenHmac> {
    let mut raw = Vec::new();
    token_data
      .serialize(&mut rmp_serde::Serializer::new(&mut raw))
      .unwrap();
    let mut mac = Hmac::<Sha256>::new_from_slice(self.secret.as_slice()).unwrap();
    mac.update(&raw);
    mac.finalize()
  }

  pub fn generate<T: Serialize>(&self, token_data: &T) -> Vec<u8> {
    self.calculate_mac(token_data).into_bytes().to_vec()
  }

  pub fn verify<T: Serialize>(&self, token: &[u8], expected_token_data: &T) -> bool {
    if token.len() != BLOBD_TOKEN_HMAC_LEN {
      return false;
    };
    let mac: [u8; BLOBD_TOKEN_HMAC_LEN] = token.try_into().unwrap();
    let mac: Output<Hmac<Sha256>> = mac.into();
    // We must use CtOutput to avoid timing attacks that defeat HMAC security.
    let mac = CtOutput::from(mac);
    if self.calculate_mac(expected_token_data) != mac {
      return false;
    };
    true
  }
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
// WARNING: Order of fields is significant, as rmp_serde will serialise in this order without field names.
pub enum AuthTokenAction {
  BatchCreateObjects {},
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
  expires: u64, // UNIX timestamp, seconds since epoch.
}

impl AuthToken {
  pub fn new(tokens: &BlobdTokens, action: AuthTokenAction, expires: u64) -> String {
    let token_data = AuthToken { action, expires };
    let token_raw = tokens.generate(&token_data);
    let mut raw = expires.to_be_bytes().to_vec();
    raw.extend_from_slice(&token_raw);
    BASE64URL_NOPAD.encode(&raw)
  }

  pub fn verify(tokens: &BlobdTokens, token: &str, expected_action: AuthTokenAction) -> bool {
    let now = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .expect("system clock is before 1970")
      .as_secs();
    let Ok(raw) = BASE64URL_NOPAD.decode(token.as_bytes()) else {
      return false;
    };
    if raw.len() != 8 + BLOBD_TOKEN_HMAC_LEN {
      return false;
    };
    let (expires_raw, token_raw) = raw.split_at(size_of::<u64>());
    let expires = u64::from_be_bytes(expires_raw.try_into().unwrap());
    if !tokens.verify(token_raw, &AuthToken {
      action: expected_action,
      expires,
    }) {
      return false;
    };
    if expires <= now {
      return false;
    };
    true
  }
}

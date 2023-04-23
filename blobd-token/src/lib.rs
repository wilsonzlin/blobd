use blake3::Hash;
use data_encoding::BASE64URL_NOPAD;
use serde::Deserialize;
use serde::Serialize;
use std::mem::size_of;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

pub struct BlobdTokens {
  secret: [u8; 32],
}

const BLOBD_TOKEN_MAC_LEN: usize = 32;

impl BlobdTokens {
  pub fn new(secret: [u8; BLOBD_TOKEN_MAC_LEN]) -> Self {
    Self { secret }
  }

  fn calculate_mac<T: Serialize>(&self, token_data: &T) -> Hash {
    let mut raw = Vec::new();
    token_data
      .serialize(&mut rmp_serde::Serializer::new(&mut raw))
      .unwrap();
    blake3::keyed_hash(&self.secret, &raw)
  }

  pub fn generate<T: Serialize>(&self, token_data: &T) -> Vec<u8> {
    self.calculate_mac(token_data).as_bytes().to_vec()
  }

  pub fn verify<T: Serialize>(&self, token: &[u8], expected_token_data: &T) -> bool {
    if token.len() != BLOBD_TOKEN_MAC_LEN {
      return false;
    };
    let mac: [u8; BLOBD_TOKEN_MAC_LEN] = token.try_into().unwrap();
    // We must use Hash to avoid timing attacks that defeat MAC security.
    let mac: Hash = mac.into();
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
  // TODO Support incomplete_token.
  CommitObject { key: Vec<u8> },
  CreateObject { key: Vec<u8>, size: u64 },
  DeleteObject { key: Vec<u8> },
  InspectObject { key: Vec<u8> },
  ReadObject { key: Vec<u8> },
  // TODO Support incomplete_token.
  WriteObject { key: Vec<u8> },
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
    if raw.len() != 8 + BLOBD_TOKEN_MAC_LEN {
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

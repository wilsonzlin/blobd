use data_encoding::BASE64URL_NOPAD;
use hmac::digest::CtOutput;
use hmac::digest::Output;
use hmac::Hmac;
use hmac::Mac;
use rand::thread_rng;
use rand::RngCore;
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

const BLOBD_HMAC_SALT_LEN: usize = 16;

impl BlobdTokens {
  pub fn new(secret: [u8; 32]) -> Self {
    Self { secret }
  }

  fn calculate_mac(&self, salt_and_token_data: &[u8]) -> CtOutput<BlobdTokenHmac> {
    let mut mac = Hmac::<Sha256>::new_from_slice(self.secret.as_slice()).unwrap();
    mac.update(salt_and_token_data);
    mac.finalize()
  }

  pub fn generate<T: Serialize>(&self, token_data: T) -> Vec<u8> {
    let mut salt = [0u8; BLOBD_HMAC_SALT_LEN];
    thread_rng().fill_bytes(&mut salt);
    let mut raw = salt.to_vec();
    token_data
      .serialize(&mut rmp_serde::Serializer::new(&mut raw))
      .unwrap();
    let mac = self.calculate_mac(&raw).into_bytes();
    raw.splice(0..0, mac);
    raw
  }

  pub fn parse_and_verify<'de, T: Deserialize<'de>>(&self, token: &'de [u8]) -> Option<T> {
    if token.len() < BLOBD_TOKEN_HMAC_LEN + BLOBD_HMAC_SALT_LEN {
      return None;
    };
    let (mac, raw) = token.split_at(BLOBD_TOKEN_HMAC_LEN);
    let mac: [u8; BLOBD_TOKEN_HMAC_LEN] = mac.try_into().unwrap();
    let mac: Output<Hmac<Sha256>> = mac.into();
    // We must use CtOutput to avoid timing attacks that defeat HMAC security.
    let mac = CtOutput::from(mac);
    if self.calculate_mac(raw) != mac {
      return None;
    };
    // The salt has already been verified as the HMAC check has passed.
    let (_salt, raw) = raw.split_at(BLOBD_HMAC_SALT_LEN);
    rmp_serde::decode::from_slice(raw).ok()
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
  expires: u64, // UNIX timestamp, seconds since epoch.
}

impl AuthToken {
  pub fn new(tokens: &BlobdTokens, action: AuthTokenAction, expires: u64) -> String {
    let token_data = AuthToken { action, expires };
    let token_raw = tokens.generate(token_data);
    BASE64URL_NOPAD.encode(&token_raw)
  }

  pub fn verify(tokens: &BlobdTokens, token: &str, expected_action: AuthTokenAction) -> bool {
    let now = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .expect("system clock is before 1970")
      .as_secs();
    let Ok(token_raw) = BASE64URL_NOPAD.decode(token.as_bytes()) else {
      return false;
    };
    let Some(v): Option<AuthToken> = tokens.parse_and_verify(&token_raw) else {
      return false;
    };
    v.expires > now && v.action == expected_action
  }
}

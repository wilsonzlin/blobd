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

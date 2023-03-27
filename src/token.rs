use hmac::digest::CtOutput;
use hmac::digest::Output;
use hmac::Hmac;
use hmac::Mac;
use sha2::Sha256;

pub struct BlobdTokens {
  secret: [u8; 32],
}

impl BlobdTokens {
  pub fn new(secret: [u8; 32]) -> Self {
    Self { secret }
  }

  fn calculate(&self, data: &[u8]) -> CtOutput<Hmac<Sha256>> {
    let mut mac = Hmac::<Sha256>::new_from_slice(self.secret.as_slice()).unwrap();
    mac.update(data);
    mac.finalize()
  }

  pub fn generate(&self, data: &[u8]) -> Vec<u8> {
    self.calculate(data).into_bytes().to_vec()
  }

  pub fn verify(&self, data: &[u8], token: &[u8]) -> bool {
    let Some(token): Option<[u8; 32]> = token.try_into().ok() else {
      return false;
    };
    let token: Output<Hmac<Sha256>> = token.into();
    // We must use CtOutput to avoid timing attacks that defeat HMAC security.
    let token = CtOutput::from(token);
    self.calculate(data) == token
  }
}

#[cfg(test)]
mod test {
  use super::BlobdTokens;
  use hex_literal::hex;

  #[test]
  fn test_token_generation_and_verification() {
    let secret = [
      0xc1, 0x18, 0xbc, 0xb4, 0x0d, 0x18, 0xda, 0x02, 0xa3, 0x6e, 0x05, 0x97, 0xff, 0xfd, 0x1d,
      0x7c, 0x90, 0xc3, 0x1b, 0x60, 0xe5, 0x3c, 0x60, 0x15, 0xd8, 0x92, 0x6d, 0x27, 0xd3, 0xe4,
      0x52, 0x3c,
    ];
    let tokens = BlobdTokens::new(secret);
    let data = hex!("619fc9c971210fe022e7ed6087e3978485d37b9eeb3b9895bd5a9a4b4293511c6e94c513848ee07e2022668b922af94de1ba609900627d9ea78a1006f61f56e4566105df5981952328d7c35fbf891961bb0a192b98f9dea542e9243cdf050939845389aa9f6ffaae3142c3fb6fb0755996ef48813afef1dced2cc6c0f6689543c990e5a9d5f195c555091a49792995e1ca8a126e097d76866a00dc901f7ded4be6d1c7ff993af7cb1903c44bef6af7ed19e2daa9f348c48c92bb43e63fcaecaa84ac5f842fd0bc2344a4e943bb8d62ef4f8b386e8f");
    let expected_mac = hex!("76b0cf12c5a90e9d01d8cabde546fefde77080e18fdc8f8cc40a31e99c3644b0");
    assert_eq!(tokens.generate(&data), expected_mac);
    assert!(tokens.verify(&data, &expected_mac));
  }
}

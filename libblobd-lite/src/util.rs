#![allow(unused)]

use chrono::Utc;
use off64::u32;
use off64::u64;

pub(crate) fn get_now_ms() -> u64 {
  u64!(Utc::now().timestamp_millis())
}

pub(crate) fn get_now_sec() -> u64 {
  u64!(Utc::now().timestamp())
}

pub(crate) fn get_now_hour() -> u32 {
  u32!(Utc::now().timestamp() / 60 / 60)
}

pub(crate) fn div_ceil(n: u64, d: u64) -> u64 {
  (n / d) + ((n % d != 0) as u64)
}

pub(crate) fn div_mod_pow2(val: u64, pow2: u8) -> (u64, u64) {
  (val >> pow2, val & ((1 << pow2) - 1))
}

pub(crate) fn div_pow2(val: u64, pow2: u8) -> u64 {
  let (div, _) = div_mod_pow2(val, pow2);
  div
}

pub(crate) fn mod_pow2(val: u64, pow2: u8) -> u64 {
  let (_, mod_) = div_mod_pow2(val, pow2);
  mod_
}

pub(crate) fn mul_pow2(val: u64, pow2: u8) -> u64 {
  val << pow2
}

/// Round up to next `2^pow2`.
pub(crate) fn ceil_pow2(val: u64, pow2: u8) -> u64 {
  let (mut div, mod_) = div_mod_pow2(val, pow2);
  if mod_ != 0 {
    div += 1;
  };
  div << pow2
}

/// Round down to previous `2^pow2`.
pub(crate) fn floor_pow2(val: u64, pow2: u8) -> u64 {
  let (mut div, mod_) = div_mod_pow2(val, pow2);
  div << pow2
}

pub(crate) fn is_multiple_of_pow2(val: u64, pow2: u8) -> bool {
  let (_, mod_) = div_mod_pow2(val, pow2);
  mod_ == 0
}

#[cfg(test)]
mod tests {
  use crate::util::ceil_pow2;
  use crate::util::div_ceil;
  use crate::util::div_mod_pow2;

  #[test]
  fn test_div_ceil() {
    assert_eq!(div_ceil(0, 1), 0);
    assert_eq!(div_ceil(0, 2), 0);
    assert_eq!(div_ceil(0, 3), 0);
    assert_eq!(div_ceil(1, 2), 1);
    assert_eq!(div_ceil(1, 3), 1);
    assert_eq!(div_ceil(10, 3), 4);
    assert_eq!(div_ceil(2, 2), 1);
    assert_eq!(div_ceil(3, 2), 2);
  }

  #[test]
  fn test_div_mod_pow2() {
    assert_eq!(div_mod_pow2(0, 0), (0, 0));
    assert_eq!(div_mod_pow2(0, 1), (0, 0));
    assert_eq!(div_mod_pow2(0, 2), (0, 0));
    assert_eq!(div_mod_pow2(1, 0), (1, 0));
    assert_eq!(div_mod_pow2(1, 1), (0, 1));
    assert_eq!(div_mod_pow2(1, 2), (0, 1));
    assert_eq!(div_mod_pow2(5, 2), (1, 1));
    assert_eq!(div_mod_pow2(10, 2), (2, 2));
    assert_eq!(div_mod_pow2(16392, 6), (256, 8));
    assert_eq!(div_mod_pow2(16393, 6), (256, 9));
  }

  #[test]
  fn test_ceil_pow2() {
    assert_eq!(ceil_pow2(0, 0), 0);
    assert_eq!(ceil_pow2(1, 0), 1);
    assert_eq!(ceil_pow2(2, 0), 2);
    assert_eq!(ceil_pow2(3, 0), 3);
    assert_eq!(ceil_pow2(3, 1), 4);
    assert_eq!(ceil_pow2(0, 2), 0);
    assert_eq!(ceil_pow2(1, 2), 4);
    assert_eq!(ceil_pow2(3, 2), 4);
    assert_eq!(ceil_pow2(4, 2), 4);
    assert_eq!(ceil_pow2(4, 8), 256);
    assert_eq!(ceil_pow2(5, 8), 256);
  }
}

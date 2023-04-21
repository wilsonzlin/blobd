#![allow(unused)]

pub(crate) fn div_ceil(n: u64, d: u64) -> u64 {
  (n / d) + ((n % d != 0) as u64)
}

pub(crate) fn div_mod_pow2(val: u64, pow2: u8) -> (u64, u64) {
  let mask = (1 << pow2) - 1;
  (val & !mask, val & mask)
}

pub(crate) fn div_pow2(val: u64, pow2: u8) -> u64 {
  let (div, _) = div_mod_pow2(val, pow2);
  div
}

pub(crate) fn mod_pow2(val: u64, pow2: u8) -> u64 {
  let (_, mod_) = div_mod_pow2(val, pow2);
  mod_
}

// Round up to next `2^pow2`.
pub(crate) fn ceil_pow2(val: u64, pow2: u8) -> u64 {
  let (mut div, mod_) = div_mod_pow2(val, pow2);
  if mod_ != 0 {
    div += 1;
  };
  div
}

pub(crate) fn is_multiple_of_pow2(val: u64, pow2: u8) -> bool {
  let (_, mod_) = div_mod_pow2(val, pow2);
  mod_ == 0
}

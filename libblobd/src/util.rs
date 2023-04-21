pub(crate) fn div_mod_pow2(val: u64, pow2: u8) -> (u64, u64) {
  let mask = (1 << pow2) - 1;
  (val & !mask, val & mask)
}

// Round up to next `2^pow2`.
pub(crate) fn ceil_pow2(val: u64, pow2: u8) -> u64 {
  let (mut div, mod_) = div_mod_pow2(val, pow2);
  if mod_ != 0 {
    div += 1;
  };
  div
}

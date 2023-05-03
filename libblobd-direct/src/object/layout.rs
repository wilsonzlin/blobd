use super::tail::TailPageSizes;
use crate::pages::Pages;
use crate::util::ceil_pow2;
use crate::util::div_mod_pow2;
use off64::u32;
use off64::u8;

pub(crate) struct ObjectLayout {
  pub lpage_count: u32,
  pub tail_page_sizes_pow2: TailPageSizes,
}

pub(crate) fn calc_object_layout(pages: &Pages, object_size: u64) -> ObjectLayout {
  let (lpage_count, tail_size) = div_mod_pow2(object_size, pages.lpage_size_pow2);
  let lpage_count = u32!(lpage_count);
  let mut rem = ceil_pow2(tail_size, pages.spage_size_pow2);
  let mut tail_page_sizes_pow2 = TailPageSizes::new();
  loop {
    let pos = rem.leading_zeros();
    if pos == 64 {
      break;
    };
    let pow2 = u8!(63 - pos);
    tail_page_sizes_pow2.push(pow2);
    rem &= !(1 << pow2);
  }
  ObjectLayout {
    lpage_count,
    tail_page_sizes_pow2,
  }
}

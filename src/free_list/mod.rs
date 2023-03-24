use seekable_async_file::SeekableAsyncFile;

pub mod avx512;

pub trait FreeList {
  fn load_from_device(&mut self, device: SeekableAsyncFile, tile_count: u64) -> ();
  fn consume_microtile_space(&mut self, bytes_needed: u32) -> u64;
  fn consume_one_tile(&mut self) -> u32;
  fn consume_tiles(&mut self, tiles_needed: u64) -> Vec<u32>;
  fn replenish_tiles(&mut self, tiles: &[u32]) -> ();
}

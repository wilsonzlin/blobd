/**

TILES
=====

For simplicity, the entire device is split into tiles, including reserved space at the device head used by the journal, stream, free list, etc. This makes it possible to convert to/from device offsets using only division and modulo operations, instead of also adding and subtracting a runtime value (the reserved space size is unknown until runtime due to bucket count being configurable) which is slower and more error prone.

**/

pub const TILE_SIZE: u32 = 1024 * 1024 * 16;
pub const TILE_SIZE_U64: u64 = TILE_SIZE as u64;

use crate::tile::TILE_SIZE;
use crate::tile::TILE_SIZE_U64;

/**

Structure
---------

We cannot store head data inline as we need to know the inode size in order to allocate a fragment for it, but we cannot know any inline head data length unless we know the inode size. For simplicity and flexibility, we just use a fragment for tail data, instead of storing it inline which would make it subject to the free space within the same fragmented tile.

The ordering of these fields is important (and somewhat strange/seemingly random), as we want to avoid multiple small reads.
- **write_object** requires `size`, `obj_id`, `key_len`, `tail_data_fragment_dev_offset_or_zero_if_none`, and one `tile` element.
- **find_inode_in_bucket** requires `obj_id`, `key_len`, `key`, and `next_inode_dev_offset_or_zero_if_end`.
- **read_object** requires *find_inode_in_bucket* as well as `size`, `tail_data_fragment_dev_offset_or_zero_if_none`, and one `tile` element.
- **commit_object** requires `obj_id`.

u48 tail_data_fragment_dev_offset_or_zero_if_none
u40 size
u64 obj_id
u16 key_len
u48 next_inode_dev_offset_or_zero_if_end
u8[] key
u24[] tiles

**/

pub(crate) const INO_OFFSETOF_TAIL_FRAG_DEV_OFFSET: u64 = 0;
pub(crate) const INO_OFFSETOF_SIZE: u64 = INO_OFFSETOF_TAIL_FRAG_DEV_OFFSET + 6;
pub(crate) const INO_OFFSETOF_OBJ_ID: u64 = INO_OFFSETOF_SIZE + 5;
pub(crate) const INO_OFFSETOF_KEY_LEN: u64 = INO_OFFSETOF_OBJ_ID + 8;
pub(crate) const INO_OFFSETOF_NEXT_INODE_DEV_OFFSET: u64 = INO_OFFSETOF_KEY_LEN + 2;
pub(crate) const INO_OFFSETOF_KEY: u64 = INO_OFFSETOF_NEXT_INODE_DEV_OFFSET + 6;
#[allow(non_snake_case)]
pub(crate) fn INO_OFFSETOF_TILE_IDX(key_len: u16, tile_idx: u16) -> u64 {
  INO_OFFSETOF_KEY + u64::from(key_len) + 3 * u64::from(tile_idx)
}
#[allow(non_snake_case)]
pub(crate) fn INO_OFFSETOF_TILES(key_len: u16) -> u64 {
  INO_OFFSETOF_TILE_IDX(key_len, 0)
}
#[allow(non_snake_case)]
pub(crate) fn INO_SIZE(key_len: u16, tile_count: u16) -> u32 {
  INO_OFFSETOF_TILE_IDX(key_len, tile_count)
    .try_into()
    .unwrap()
}

pub(crate) struct ObjectAllocCfg {
  pub tile_count: u16,
  pub tail_len: u32,
}

pub(crate) fn get_object_alloc_cfg(object_size: u64) -> ObjectAllocCfg {
  // We only allow up to 65,536 solid tiles for a single object.
  let mut tile_count: u16 = (object_size / TILE_SIZE_U64).try_into().unwrap();
  let mut tail_len: u32 = (object_size % TILE_SIZE_U64).try_into().unwrap();
  // TODO Analyse this hyperparameter: is this mechanism useful? What are the impacts of higher/lower values?
  if tail_len >= TILE_SIZE - 128 {
    // The tail is too close to a full tile, so just allocate and use an extra tile instead.
    tile_count += 1;
    tail_len = 0;
  };
  ObjectAllocCfg {
    tile_count,
    tail_len,
  }
}

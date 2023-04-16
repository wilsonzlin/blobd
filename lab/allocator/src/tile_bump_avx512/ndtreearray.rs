use off64::usz;

// This is a data structure designed to be like:
// struct {
//   level_1: T,
//   level_2: [T; elems_per_level],
//   level_3: [[T; elems_per_level]; elems_per_level],
//   level_4: [[[T; elems_per_level]; elems_per_level]; elems_per_level],
//   ...
//   level_$levels_including_root: ...,
// }
// Various methods take a `key` that looks like `[5, 3, 2]`, which means `level_4[5][3][2]`. To get the first level, use `*root*` methods or the key `[]`.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct NDTreeArray<T: Clone + Copy + Default> {
  elems_per_level: u64,
  levels_including_root: u32,
  vals: Vec<T>,
}

impl<T: Clone + Copy + Default> NDTreeArray<T> {
  pub fn new(elems_per_level: u64, levels_including_root: u32) -> Self {
    let mut flat_count = 0;
    for i in 0..levels_including_root {
      flat_count += elems_per_level.pow(i);
    }
    Self {
      elems_per_level,
      levels_including_root,
      vals: vec![T::default(); usz!(flat_count)],
    }
  }

  fn to_raw_index<E: Into<u64>, const L: usize>(&self, key: [E; L]) -> usize {
    let l: u32 = L.try_into().unwrap();
    assert!(self.levels_including_root > l);
    let mut base = 0;
    for i in 0..l {
      base += self.elems_per_level.pow(i);
    }
    let mut offset = 0;
    for e in key {
      let e: u64 = e.into();
      offset += e;
      offset *= self.elems_per_level;
    }
    usz!(base + offset)
  }

  // Convenience method as `get` would not be able to infer generic parameter `E` with `[]`.
  pub fn root(&self) -> T {
    self.get::<u64, 0>([])
  }

  // Convenience method as `get_mut` would not be able to infer generic parameter `E` with `[]`.
  pub fn root_mut(&mut self) -> &mut T {
    self.get_mut::<u64, 0>([])
  }

  // Convenience method as `set` would not be able to infer generic parameter `E` with `[]`.
  pub fn set_root(&mut self, val: T) {
    self.set::<u64, 0>([], val);
  }

  // Convenience method as `compute` would not be able to infer generic parameter `E` with `[]`.
  pub fn compute_root<F: FnOnce(T) -> T>(&mut self, f: F) -> T {
    self.update::<u64, 0, F>([], f)
  }

  pub fn get<E: Into<u64>, const L: usize>(&self, key: [E; L]) -> T {
    self.vals[self.to_raw_index(key)]
  }

  pub fn get_ref<E: Into<u64>, const L: usize>(&self, key: [E; L]) -> &T {
    &self.vals[self.to_raw_index(key)]
  }

  pub fn get_mut<E: Into<u64>, const L: usize>(&mut self, key: [E; L]) -> &mut T {
    let key = self.to_raw_index(key);
    &mut self.vals[key]
  }

  pub fn set<E: Into<u64>, const L: usize>(&mut self, key: [E; L], val: T) -> T {
    let key = self.to_raw_index(key);
    self.vals[key] = val;
    val
  }

  pub fn update<E: Into<u64>, const L: usize, F: FnOnce(T) -> T>(
    &mut self,
    key: [E; L],
    f: F,
  ) -> T {
    let idx = self.to_raw_index(key);
    let val = &mut self.vals[idx];
    let new_val = f(*val);
    *val = new_val;
    new_val
  }
}

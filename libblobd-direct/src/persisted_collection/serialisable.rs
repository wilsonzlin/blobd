use off64::int::Off64ReadInt;
use off64::int::Off64WriteMutInt;
use std::sync::Arc;

pub(crate) type SerialisedLen = u16;

// `DeserialiseArgs` can be used to provide additional values to `deserialise`, useful when deserialising complex types from device and constructing them directly.
pub(crate) trait Serialisable {
  type DeserialiseArgs;
  const FIXED_SERIALISED_LEN: Option<SerialisedLen>;

  /// This must be fast.
  fn serialised_len(&self) -> SerialisedLen;

  fn serialise(&self, out: &mut [u8]);

  fn deserialise(args: &Self::DeserialiseArgs, raw: &[u8]) -> Self;
}

impl Serialisable for u64 {
  type DeserialiseArgs = ();

  const FIXED_SERIALISED_LEN: Option<SerialisedLen> = Some(8);

  fn serialised_len(&self) -> SerialisedLen {
    8
  }

  fn serialise(&self, out: &mut [u8]) {
    out.write_u64_le_at(0, *self)
  }

  fn deserialise(_args: &Self::DeserialiseArgs, raw: &[u8]) -> Self {
    raw.read_u64_le_at(0)
  }
}

impl<T: Serialisable> Serialisable for Arc<T> {
  type DeserialiseArgs = T::DeserialiseArgs;

  const FIXED_SERIALISED_LEN: Option<SerialisedLen> = T::FIXED_SERIALISED_LEN;

  fn serialised_len(&self) -> SerialisedLen {
    T::serialised_len(self)
  }

  fn serialise(&self, out: &mut [u8]) {
    T::serialise(self, out)
  }

  fn deserialise(args: &Self::DeserialiseArgs, raw: &[u8]) -> Self {
    let v = T::deserialise(args, raw);
    Self::new(v)
  }
}

impl<T: Serialisable> Serialisable for tokio::sync::RwLock<T> {
  type DeserialiseArgs = T::DeserialiseArgs;

  const FIXED_SERIALISED_LEN: Option<SerialisedLen> = T::FIXED_SERIALISED_LEN;

  fn serialised_len(&self) -> SerialisedLen {
    self.blocking_read().serialised_len()
  }

  fn serialise(&self, out: &mut [u8]) {
    self.blocking_read().serialise(out)
  }

  fn deserialise(args: &Self::DeserialiseArgs, raw: &[u8]) -> Self {
    let v = T::deserialise(args, raw);
    Self::new(v)
  }
}

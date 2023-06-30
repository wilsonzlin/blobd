use crate::backing_store::PartitionStore;
use crate::object::calc_object_layout;
use crate::object::ObjectMetadata;
use crate::object::ObjectState;
use crate::object::ObjectTuple;
use crate::pages::Pages;
use crate::partition::PartitionLoader;
use crate::tuples::load_tuples_from_raw_tuples_area;
use crate::util::mod_pow2;
use bufpool::buf::Buf;
use chrono::DateTime;
use chrono::Utc;
use futures::stream::iter;
use futures::Stream;
use futures::StreamExt;
use off64::usz;
use parking_lot::Mutex;
use serde::Deserialize;
use serde::Serialize;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use tinybuf::TinyBuf;

#[derive(PartialEq, Eq, Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct BlobdExporterMarker {
  object_id: u64,
  partition: usize,
}

impl Ord for BlobdExporterMarker {
  fn cmp(&self, other: &Self) -> std::cmp::Ordering {
    // WARNING: It's important for the order key to be `(object_id, partition)` and not the other way around as otherwise we'll keep hitting a single partition when iterating in order.
    self
      .object_id
      .cmp(&other.object_id)
      .then_with(|| self.partition.cmp(&other.partition))
  }
}

impl PartialOrd for BlobdExporterMarker {
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    Some(self.cmp(other))
  }
}

pub struct BlobdExportedObject {
  // This is generally a blobd internal value and not that useful, but we'll export it anyway.
  pub id: u64,
  pub key: TinyBuf,
  pub size: u64,
  pub created: DateTime<Utc>,
  pub data_stream: Pin<Box<dyn Stream<Item = Buf> + Send>>,
}

pub struct BlobdExporterEntry {
  tuple: ObjectTuple,
  partition_dev: PartitionStore,
  pages: Pages,
}

impl BlobdExporterEntry {
  pub async fn read(&self) -> BlobdExportedObject {
    let dev = self.partition_dev.clone();
    let pages = &self.pages;
    let tuple = &self.tuple;

    let metadata_raw = dev
      .read_at(
        tuple.metadata_dev_offset,
        1 << tuple.metadata_page_size_pow2,
      )
      .await;

    // Yes, rmp_serde stops reading once fully deserialised, and doesn't error if there is extra junk afterwards.
    let ObjectMetadata {
      size: object_size,
      created,
      key,
      lpage_dev_offsets,
      tail_page_dev_offsets,
    } = rmp_serde::from_slice(&metadata_raw).unwrap();
    let layout = calc_object_layout(&pages, object_size);

    let mut reads = Vec::new();
    for dev_offset in lpage_dev_offsets {
      reads.push((dev_offset, pages.lpage_size()));
    }
    for (i, sz) in layout.tail_page_sizes_pow2 {
      reads.push((tail_page_dev_offsets[usz!(i)], 1 << sz));
    }
    let last_read = reads.pop();
    let last_read_trunc = mod_pow2(object_size, pages.spage_size_pow2);

    let stream = async_stream::stream! {
      for (offset, len) in reads {
        yield dev.read_at(offset, len).await;
      };
      if let Some((offset, len)) = last_read {
        let mut last_chunk = dev.read_at(offset, len).await;
        if last_read_trunc > 0 {
          last_chunk.truncate(usz!(last_read_trunc));
        };
        yield last_chunk;
      };
    };

    BlobdExportedObject {
      created,
      id: tuple.id,
      key,
      size: object_size,
      data_stream: Box::pin(stream),
    }
  }
}

// TODO Document how to handle multiple committed objects with the same key.
pub struct BlobdExporter {
  entries: VecDeque<(BlobdExporterMarker, BlobdExporterEntry)>,
}

impl BlobdExporter {
  pub(crate) async fn new(
    partitions: &[PartitionLoader],
    pages: &Pages,
    offset: BlobdExporterMarker,
  ) -> BlobdExporter {
    let raw_tuple_areas: Arc<Mutex<Vec<(usize, Buf)>>> = Default::default();
    iter(partitions.iter().enumerate())
      .for_each_concurrent(None, |(id, p)| {
        let raw_tuple_areas = raw_tuple_areas.clone();
        async move {
          // Don't inline this expression, we should not hold the lock until we've read this.
          let raw = p.load_raw_tuples_area().await;
          raw_tuple_areas.lock().push((id, raw));
        }
      })
      .await;
    let mut entries = Vec::new();
    for (part_id, raw) in Arc::into_inner(raw_tuple_areas).unwrap().into_inner() {
      load_tuples_from_raw_tuples_area(&raw, pages, |_, tuple| {
        let marker = BlobdExporterMarker {
          object_id: tuple.id,
          partition: part_id,
        };
        if marker >= offset && tuple.state == ObjectState::Committed {
          entries.push((marker, BlobdExporterEntry {
            tuple,
            pages: pages.clone(),
            partition_dev: partitions[part_id].dev.clone(),
          }));
        };
      });
    }
    entries.sort_unstable_by_key(|e| e.0);
    Self {
      entries: entries.into(),
    }
  }

  pub fn pop(&mut self) -> Option<(BlobdExporterMarker, BlobdExporterEntry)> {
    self.entries.pop_front()
  }

  pub fn take(self) -> VecDeque<(BlobdExporterMarker, BlobdExporterEntry)> {
    self.entries
  }
}

# Persisted collection

Data structures that store their entries on the device as a simple immutable list, chunked by a linked list of spages. Each entry is a pair of the form (K, V).

When an entry is changed, their spage is marked as dirty. At commit time, if two or more dirty spages can be merged (i.e. all are sparse enough to fit on one), they are. They are then written to the device using the journal.

These are ideal because:
- The device only supports writing spages or larger, so there is no smaller write size even if the amount of data changed is smaller.
- We take advantage of the minimum write size requirement to perform compaction and reclaim space from deleted entries without the use of complex algorithms or data structures and with minimal writes.
- We load all data into memory, so there's no need for an optimised layout on the device.

## Types

We implement `Persisted{BTreeMap,HashMap,DashMap}` types instead of defining a `MapLike` trait and using a generic `PersistedCollection` that accepts a type that implements `MapLike`, because it gets too complex:

- Some maps don't even have the same return type for the core methods: `get()`, `get_mut()`, etc. For example, DashMap returns a `Ref`, not a standard reference, for `get`.
- With a different type per data structure, we can have more optimised, ergonomic methods, like `entry` for maps, range methods for BTreeMap values, etc.

## Keys

For map types, the key must be Copy, because there will be a lot of copying around for internal tracking, so if it's not cheap to copy then CPU and memory may explode.

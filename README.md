# blobd

Extremely fast and parallel object storage, performing at raw device speeds. Designed for millions of concurrent random partial reads over trillions of objects (tiny or huge) with constant latency.

- All the good stuff: shared-nothing architecture, io_uring, async Rust for I/O, no page cache, direct I/O.
- Guaranteed durability: safe to crash at any time, and a success result means the data has been 100% persisted.
- Create partitions to locally shard within or across block devices or files for extremely high create and delete throughput.
- Available as embedded library, RPC server, or HTTP RESTful server with CORS, range requests, presigned URLs, and HTTP/2.
- Asynchronous replication and event streaming.

## Design

- On-device configurable fixed-size hash map with linked list of objects on the heap. The entire device is mapped to memory.
- Optimised for reads, then creates, then deletes. There is no way to list objects.
- Create an object, then concurrently write its data in 16 MiB parts, then commit it.
- Objects are immutable once committed. Versioning is currently not possible. An object replaces all other objects with the same key when it's committed (not created).
- Only the size is stored with an object. No other metadata is collected, and custom metadata cannot be set.
- The device must be under 256 TiB. Objects are limited to 1 TiB. The peak optimal amount of objects stored is around 140 trillion.
- Uncommitted objects may be deleted after 7 days. Space used by objects may not be immediately freed when the object is deleted.

## History

This project used to be called *Turbostore* and was written entirely in C; you can still see the code [here](https://github.com/wilsonzlin/blobd/tree/ffb637ae4e4e91602ec04cf2fb2b50aafa116876).

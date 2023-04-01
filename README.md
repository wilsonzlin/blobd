# blobd

- Scales to millions of random concurrent partial reads over trillions of objects (tiny or huge) at constant disk-level latency.
- Asynchronous replication and event streaming.
- Batch creation API for very high transfer and creation rates with many small objects.
- HTTP RESTful API with support for CORS, range requests, presigned URLs, and HTTP/2.

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

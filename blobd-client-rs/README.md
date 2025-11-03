# blobd-client-rs

Rust client for [blobd](https://github.com/wilsonzlin/blobd), blob storage designed for huge amounts of random reads and small objects with constant latency.

[Blog post](https://blog.wilsonl.in/blobd) · [Crates.io](https://crates.io/crates/blobd-client-rs)

## Installation

```toml
[dependencies]
blobd-client-rs = "0.7.0"
```

## Usage

```rust
use blobd_client_rs::BlobdClient;

let token_secret: [u8; 32] = [/* your 32-byte secret */];
let client = BlobdClient::new(
    "http://localhost:8080".to_string(),
    token_secret,
);
```

The client is not `Clone`. Wrap it in an `Arc` to share across threads:

```rust
use std::sync::Arc;
let client = Arc::new(client);
```

### Uploading objects

```rust
use bytes::Bytes;
use futures::future::join_all;

// Upload large object with parts in parallel
let total_size = 50 * 1024 * 1024; // 50MB
let creation = client.create_object("large/file", total_size).await?;

let chunk_size = 16 * 1024 * 1024; // 16MB chunks
let mut write_futures = Vec::new();

for offset in (0..total_size).step_by(chunk_size) {
    let chunk = get_data_chunk(offset, chunk_size); // Your data source
    let client = client.clone(); // Arc clone
    let creation = creation.clone();
    write_futures.push(async move {
        client.write_object("large/file", creation, offset, chunk).await
    });
}

let receipts = join_all(write_futures).await;
let receipt_strings: Vec<_> = receipts
    .into_iter()
    .collect::<Result<Vec<_>, _>>()?
    .into_iter()
    .map(|r| r.write_receipt)
    .collect();

client.commit_object("large/file", creation, receipt_strings).await?;
```

### Reading objects

```rust
use futures::StreamExt;

// Read entire object
let mut stream = client.read_object("my/key", None, None).await?;
let mut data = Vec::new();
while let Some(chunk) = stream.next().await {
    data.extend_from_slice(&chunk?);
}
```

```rust
// Read partial range (bytes 1000-2000)
let stream = client.read_object("my/key", Some(1000), Some(2000)).await?;
```

### Batch operations

```rust
use futures::stream::{iter, once};
use tokio::fs::File;
use tokio::io::AsyncReadExt;

// Upload many objects at once from diverse sources
let mut objects = Vec::new();

// From buffer
objects.push(BatchCreateObjectEntry {
    key: b"batch/from-buffer".to_vec(),
    size: 100,
    data_stream: once(async { Ok(Bytes::from(vec![0u8; 100])) }),
});

// From file stream
let mut file = File::open("data.bin").await?;
let file_size = file.metadata().await?.len();
let file_stream = async_stream::stream! {
    let mut buffer = vec![0u8; 8192];
    loop {
        match file.read(&mut buffer).await {
            Ok(0) => break,
            Ok(n) => yield Ok(Bytes::copy_from_slice(&buffer[..n])),
            Err(e) => {
                yield Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>);
                break;
            }
        }
    }
};
objects.push(BatchCreateObjectEntry {
    key: b"batch/from-file".to_vec(),
    size: file_size,
    data_stream: Box::pin(file_stream),
});

// From buffer
objects.push(BatchCreateObjectEntry {
    key: b"batch/generated".to_vec(),
    size: 1024,
    data_stream: once(async { Ok(Bytes::from(vec![42u8; 1024])) }),
});

let result = client.batch_create_objects(iter(objects), None).await?;
println!("Created {} objects", result.successful_count);
```

### Other operations

```rust
use blobd_token::AuthTokenAction;

// Get object metadata
let info = client.inspect_object("my/key").await?;
println!("Size: {}, ID: {}", info.content_length, info.object_id);

// Delete object
client.delete_object("my/key").await?;

// Generate presigned URL
let url = client.generate_presigned_url(
    "my/key",
    AuthTokenAction::ReadObject { key: b"my/key".to_vec() },
    3600, // expires in 1 hour
);
```

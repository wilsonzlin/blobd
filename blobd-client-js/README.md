# @blobd/client

TypeScript client for [blobd](https://github.com/wilsonzlin/blobd), blob storage designed for huge amounts of random reads and small objects with constant latency.

[Blog post](https://blog.wilsonl.in/blobd) · [npm](https://www.npmjs.com/package/@blobd/client)

## Installation

```bash
npm install @blobd/client
```

## Usage

```typescript
import { BlobdClient } from "@blobd/client";

const client = new BlobdClient(
  "http://localhost:8080",
  Buffer.from("your-32-byte-token-secret-base64", "base64")
);
```

### Uploading objects

```typescript
// Upload large object with parts in parallel
const totalSize = 50 * 1024 * 1024; // 50MB
const creation = await client.createObject("large/file", totalSize);

const chunkSize = 16 * 1024 * 1024; // 16MB chunks
const writePromises = [];

for (let offset = 0; offset < totalSize; offset += chunkSize) {
  const chunk = getDataChunk(offset, chunkSize); // Your data source
  writePromises.push(
    client.writeObject("large/file", creation, offset, chunk)
  );
}

const receipts = await Promise.all(writePromises);
await client.commitObject(
  "large/file",
  creation,
  receipts.map((r) => r.writeReceipt)
);
```

### Reading objects

```typescript
// Read entire object
const stream = await client.readObject("my/key");
const reader = stream.getReader();
const chunks = [];
while (true) {
  const { done, value } = await reader.read();
  if (done) break;
  chunks.push(value);
}
const data = Buffer.concat(chunks);
```

```typescript
// Read partial range (bytes 1000-2000)
const stream = await client.readObject("my/key", 1000, 2000);
```

### Batch operations

```typescript
// Upload many objects at once from diverse sources
import { createReadStream } from "fs";

const objects = [
  // From buffer
  {
    key: "batch/from-buffer",
    size: 100,
    data: Buffer.from("..."),
  },
  // From stream
  {
    key: "batch/from-file",
    size: fileSize,
    data: new ReadableStream({
      start(controller) {
        const stream = createReadStream("file.dat");
        stream.on("data", (chunk) => controller.enqueue(new Uint8Array(chunk)));
        stream.on("end", () => controller.close());
        stream.on("error", (err) => controller.error(err));
      },
    }),
  },
];

const result = await client.batchCreateObjects(objects);
console.log(`Created ${result.successfulCount} objects`);
```

### Other operations

```typescript
// Get object metadata
const info = await client.inspectObject("my/key");
console.log(`Size: ${info.contentLength}, ID: ${info.objectId}`);

// Delete object
await client.deleteObject("my/key");

// Generate presigned URL
const url = client.generatePresignedUrl(
  "my/key",
  { type: "ReadObject", key: Buffer.from("my/key") },
  3600 // expires in 1 hour
);
```

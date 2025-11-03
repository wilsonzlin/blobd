import { test } from "vitest";
import { BlobdClient } from "./main";
import crypto from "crypto";

const key = (no: number) => `/random/data/${no}`;

const SIZE_MIN = +process.env["SIZE_MIN"]! || 12345;
const SIZE_MAX = +process.env["SIZE_MAX"]! || 123456789;
const CONCURRENCY = +process.env["CONCURRENCY"]! || 1;
const COUNT = +process.env["COUNT"]! || 1;
const ITERATIONS = +process.env["ITERATIONS"]! || 2;
const BATCH_COUNT = +process.env["BATCH_COUNT"]! || 10;
const DATA = process.env["DATA"] ?? "rand";
const ENDPOINT = process.env["BLOBD_ENDPOINT"] ?? "http://127.0.0.1:9981";
const TOKEN_SECRET: Buffer = process.env["BLOBD_TOKEN_SECRET"]
  ? Buffer.from(process.env["BLOBD_TOKEN_SECRET"], "base64")
  : crypto.randomBytes(32); // For testing without auth

const dataPool =
  DATA === "alpha"
    ? Buffer.from(
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".repeat(
          Math.ceil(SIZE_MAX / 62)
        )
      )
    : DATA === "rand"
    ? crypto.randomBytes(1024 * 1024 * 1024 * 1)
    : Buffer.alloc(0);

const ensureEqualData = (expected: Buffer, received: Uint8Array) => {
  if (expected.equals(received)) {
    return;
  }

  for (let i = 0; i < expected.length; i++) {
    if (expected[i] !== received[i]) {
      const ch = (num: number) => String.fromCharCode(num);
      const repr = (buf: Uint8Array) => [...buf].map((c) => ch(c)).join(" ");
      const ctx = (data: Uint8Array, i: number) =>
        [
          repr(data.slice(i - 16, i)),
          `[${ch(data[i])}]`,
          repr(data.slice(i + 1, i + 17)),
        ].join(" ");
      throw new Error(
        [
          `Bytes differ at offset ${i} (size ${expected.length}):`,
          `Expected: ${ctx(expected, i)}`,
          `Received: ${ctx(received, i)}`,
        ].join("\n")
      );
    }
  }
};

const readBufferStream = async (
  stream: ReadableStream<Uint8Array>
): Promise<Buffer> => {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }

  return Buffer.concat(chunks);
};

test(
  "uploading and downloading works",
  async () => {
    const pool = Array.from(
      { length: CONCURRENCY },
      () => new BlobdClient(ENDPOINT, TOKEN_SECRET)
    );

    for (let i = 0; i < ITERATIONS; i++) {
      const keyToExpectedData = new Map();
      const promises: Promise<void>[] = [];

      // Upload phase
      for (let no = 0; no < COUNT; no++) {
        const conn = pool[no % pool.length];
        const k = key(no);
        const promise = (async () => {
          const size = crypto.randomInt(SIZE_MIN, SIZE_MAX + 1);
          const creation = await conn.createObject(k, size);

          let data: Buffer;
          switch (DATA) {
            case "alpha":
              data = dataPool.slice(0, size);
              break;
            case "rand": {
              const dataPoolStart = crypto.randomInt(0, dataPool.length - size);
              data = dataPool.slice(dataPoolStart, dataPoolStart + size);
              break;
            }
            default:
              throw new Error(`Unknown data type: ${DATA}`);
          }
          keyToExpectedData.set(k, data);

          // Write object in chunks and collect receipts
          const writeReceipts: string[] = [];
          const chunkSize = 16777216; // 16MB chunks
          for (let start = 0; start < size; start += chunkSize) {
            const end = Math.min(start + chunkSize, size);
            const chunk = data.slice(start, end);
            const result = await conn.writeObject(k, creation, start, chunk);
            writeReceipts.push(result.writeReceipt);
          }

          await conn.commitObject(k, creation, writeReceipts);
        })();
        promises.push(promise);
      }

      await Promise.all(promises);

      // Read phase
      const readPromises: Promise<void>[] = [];
      for (let no = 0; no < COUNT; no++) {
        const conn = pool[no % pool.length];
        const k = key(no);
        const promise = (async () => {
          const data = keyToExpectedData.get(k);
          if (!data) {
            // We previously failed to even create an object with this key.
            return;
          }

          // Test 10 random subrange reads of the object data.
          for (let j = 0; j < 10; j++) {
            const offset = crypto.randomInt(data.length);
            const length = crypto.randomInt(data.length - offset) + 1;
            const dataSlice = data.slice(offset, offset + length);
            const read = await conn.readObject(k, offset, offset + length);
            const rd = await readBufferStream(read);
            ensureEqualData(dataSlice, rd);
          }

          // Test full read of object data.
          const fullRead = await conn.readObject(k, 0, data.length);
          ensureEqualData(data, await readBufferStream(fullRead));
        })();
        readPromises.push(promise);
      }

      await Promise.all(readPromises);
    }
  },
  { timeout: 2 ** 31 - 1 }
);

test(
  "batch create objects works",
  async () => {
    const client = new BlobdClient(ENDPOINT, TOKEN_SECRET);
    const batchKey = (no: number) => `/batch/test/${no}`;

    // Prepare batch objects with small sizes for faster testing
    const objectsToCreate: Array<{
      key: string;
      size: number;
      data: Buffer;
    }> = [];

    for (let i = 0; i < BATCH_COUNT; i++) {
      const size = crypto.randomInt(1024, 65536); // 1KB to 64KB
      let data: Buffer;
      switch (DATA) {
        case "alpha":
          data = dataPool.slice(0, size);
          break;
        case "rand": {
          const dataPoolStart = crypto.randomInt(0, dataPool.length - size);
          data = dataPool.slice(dataPoolStart, dataPoolStart + size);
          break;
        }
        default:
          throw new Error(`Unknown data type: ${DATA}`);
      }
      objectsToCreate.push({
        key: batchKey(i),
        size,
        data,
      });
    }

    // Test batch creation
    let totalBytesTransferred = 0;
    const result = await client.batchCreateObjects(
      objectsToCreate.map((obj) => ({
        key: obj.key,
        size: obj.size,
        data: obj.data,
      })),
      (bytes) => {
        totalBytesTransferred += bytes;
      }
    );

    // Verify all objects were created
    if (result.successfulCount !== BATCH_COUNT) {
      throw new Error(
        `Expected ${BATCH_COUNT} objects to be created, but got ${result.successfulCount}`
      );
    }

    // Verify transfer byte counter was called
    const expectedBytes = objectsToCreate.reduce(
      (sum, obj) => sum + obj.size + 2 + obj.key.length + 5,
      0
    );
    if (totalBytesTransferred < expectedBytes) {
      throw new Error(
        `Expected at least ${expectedBytes} bytes transferred, but got ${totalBytesTransferred}`
      );
    }

    // Verify all objects can be read back with correct data
    const readPromises = objectsToCreate.map(async (obj) => {
      const stream = await client.readObject(obj.key, 0, obj.size);
      const readData = await readBufferStream(stream);
      ensureEqualData(obj.data, readData);
    });

    await Promise.all(readPromises);
  },
  { timeout: 60000 }
);

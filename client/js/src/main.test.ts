import readBufferStream from "@xtjs/lib/js/readBufferStream";
import waitGroup from "@xtjs/lib/js/waitGroup";
import { TurbostoreClient } from "./main";
import crypto from "crypto";

const key = (no: number) => `/random/data/${no}`;

const SIZE_MIN = 12345;
const SIZE_MAX = 123456789;
const CONCURRENCY = +process.env["CONCURRENCY"]! || 1;
const COUNT = +process.env["COUNT"]! || 1;
const ITERATIONS = +process.env["ITERATIONS"]! || 2;
const DATA = process.env["DATA"] ?? "rand";

const dataPool =
  DATA == "alpha"
    ? Buffer.from(
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".repeat(
          Math.ceil(SIZE_MAX / 62)
        )
      )
    : DATA == "rand"
    ? crypto.randomBytes(1024 * 1024 * 1024 * 1)
    : Buffer.alloc(0);

jest.setTimeout(120000);

test("uploading and downloading works", async () => {
  const pool = Array.from(
    { length: CONCURRENCY },
    () =>
      new TurbostoreClient({
        host: "127.0.0.1",
        port: 9001,
      })
  );
  for (let i = 0; i < ITERATIONS; i++) {
    const keyToExpectedData = new Map();
    let wg = waitGroup();
    for (let no = 0; no < COUNT; no++) {
      wg.add();
      const conn = pool[no % pool.length];
      const k = key(no);
      (async () => {
        const size = crypto.randomInt(SIZE_MIN, SIZE_MAX + 1);
        const { inodeDeviceOffset, objectNumber } = await conn.createObject(
          k,
          size
        );
        let data;
        switch (DATA) {
          case "alpha":
            data = dataPool.slice(0, size);
            break;
          case "inode":
            const raw = `_${inodeDeviceOffset}`;
            data = Buffer.from(
              raw.repeat(Math.ceil(size / raw.length)).slice(0, size)
            );
            break;
          case "rand":
            const dataPoolStart = crypto.randomInt(0, dataPool.length - size);
            data = dataPool.slice(dataPoolStart, dataPoolStart + size);
            break;
          default:
            throw new Error(`Unknown data type: ${DATA}`);
        }
        keyToExpectedData.set(k, data);
        for (let start = 0; start < size; start += 16777216) {
          await conn.writeObjectWithBuffer(
            inodeDeviceOffset,
            objectNumber,
            start,
            data.slice(start, start + 16777216)
          );
        }
        await conn.commitObject(inodeDeviceOffset, objectNumber);
      })().finally(() => wg.done());
    }
    await wg;
    wg = waitGroup();
    // We cannot read across iterations as objects may get deleted by future iterations of the same key before we're able to read them.
    for (let no = 0; no < COUNT; no++) {
      wg.add();
      const conn = pool[no % pool.length];
      const k = key(no);
      (async () => {
        const data = keyToExpectedData.get(k);
        if (!data) {
          // We previously failed to even create an object with this key.
          return;
        }
        const read = await conn.readObject(k, 0, data.length);
        const rd = await readBufferStream(read.stream);
        if (read.actualStart !== 0 || read.actualLength !== data.length) {
          throw new Error(
            `Invalid read (wanted length ${data.length}, got length ${read.actualLength})`
          );
        }
        if (!data.equals(rd)) {
          for (let i = 0; i < data.length; i++) {
            if (data[i] != rd[i]) {
              const ch = (num: number) => String.fromCharCode(num);
              const repr = (buf: Uint8Array) =>
                [...buf].map((c) => ch(c)).join(" ");
              const ctx = (data: Uint8Array, i: number) =>
                [
                  repr(data.slice(i - 16, i)),
                  `[${ch(data[i])}]`,
                  repr(data.slice(i + 1, i + 17)),
                ].join(" ");
              throw new Error(
                [
                  `Bytes differ at offset ${i} (size ${data.length}):`,
                  `Expected: ${ctx(data, i)}`,
                  `Received: ${ctx(rd, i)}`,
                ].join("\n")
              );
            }
          }
        }
      })().finally(() => wg.done());
    }
    await wg;
  }
  await Promise.all(pool.map((conn) => conn.close()));
});

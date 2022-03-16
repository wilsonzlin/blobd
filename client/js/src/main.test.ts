import readBufferStream from "@xtjs/lib/js/readBufferStream";
import waitGroup from "@xtjs/lib/js/waitGroup";
import { TurbostoreClient } from "./main";
import crypto from "crypto";

const key = (no: number) => `/random/data/${no}`;

const SIZE_MIN = 12345;
const SIZE_MAX = 123456789;
const CONCURRENCY = +process.env["CONCURRENCY"]! || 1;
const COUNT = +process.env["COUNT"]! || 1;
const USE_ALPHA_DATA = process.env["USE_ALPHA_DATA"] === "1";

const dataPool = USE_ALPHA_DATA
  ? Buffer.from(
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".repeat(
        Math.ceil(SIZE_MAX / 62)
      )
    )
  : crypto.randomBytes(1024 * 1024 * 1024 * 1);

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
  const wg = waitGroup();
  for (let no = 0; no < COUNT; no++) {
    wg.add();
    const conn = pool[no % pool.length];
    const k = key(no);
    (async () => {
      const size = crypto.randomInt(SIZE_MIN, SIZE_MAX + 1);
      const dataPoolStart = USE_ALPHA_DATA
        ? 0
        : crypto.randomInt(0, dataPool.length - size);
      const data = dataPool.slice(dataPoolStart, dataPoolStart + size);
      const { inodeDeviceOffset, objectNumber } = await conn.createObject(
        k,
        size
      );
      for (let start = 0; start < size; start += 16777216) {
        await conn.writeObjectWithBuffer(
          inodeDeviceOffset,
          objectNumber,
          start,
          data.slice(start, start + 16777216)
        );
      }
      await conn.commitObject(inodeDeviceOffset, objectNumber);
      const read = await conn.readObject(k, 0, size);
      const rd = await readBufferStream(read.stream);
      if (read.actualStart !== 0 || read.actualLength !== data.length) {
        throw new Error(
          `Invalid read (wanted length ${data.length}, got length ${read.actualLength})`
        );
      }
      if (!data.equals(rd)) {
        for (let i = 0; i < size; i++) {
          if (data[i] != rd[i]) {
            const ch = (num: number) => String.fromCharCode(num);
            const repr = (buf: Uint8Array) =>
              [...buf].map((c) => ch(c)).join(" ");
            throw new Error(
              [
                `Bytes differ at offset ${i} (size ${size}):`,
                `Expected: ${repr(data.slice(i - 16, i))} [${ch(
                  data[i]
                )}] ${repr(data.slice(i + 1, i + 17))}`,
                `Received: ${repr(rd.slice(i - 16, i))} [${ch(rd[i])}] ${repr(
                  rd.slice(i + 1, i + 17)
                )}`,
              ].join("\n")
            );
          }
        }
      }
    })().finally(() => wg.done());
  }
  await wg;
  await Promise.all(pool.map((conn) => conn.close()));
});

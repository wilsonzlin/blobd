import readBufferStream from "@xtjs/lib/js/readBufferStream";
import waitGroup from "@xtjs/lib/js/waitGroup";
import { TurbostoreClient } from "./main";
import crypto from "crypto";

const key = (no: number) => `/random/data/${no}`;

const SIZE_MIN = 12345;
const SIZE_MAX = 123456789;
const CONCURRENCY = 8192;
const COUNT = 123456;

const dataPool = crypto.randomBytes(1024 * 1024 * 1024 * 2);

(async () => {
  const size = crypto.randomInt(SIZE_MIN, SIZE_MAX + 1);
  const dataPoolStart = crypto.randomInt(0, dataPool.length - size);
  const data = dataPool.slice(dataPoolStart, dataPoolStart + size);
  const pool = Array.from(
    { length: CONCURRENCY },
    () =>
      new TurbostoreClient({
        host: "127.0.0.1",
        port: 9001,
        onSocketError: console.error,
      })
  );
  const wg = waitGroup();
  for (let no = 0; no < COUNT; no++) {
    wg.add();
    const conn = pool[no % pool.length];
    const k = key(no);
    (async () => {
      let phase;
      try {
        phase = "create";
        const { objectNumber } = await conn.createObject(k, size);
        phase = "write";
        for (let start = 0; start < size; start += 16777216) {
          await conn.writeObjectWithBuffer(
            k,
            objectNumber,
            start,
            data.slice(start, start + 16777216)
          );
        }
        phase = "commit";
        await conn.commitObject(k, objectNumber);
        phase = "read";
        const read = await conn.readObject(k, 0, size);
        const readData = await readBufferStream(read.stream);
        if (
          read.actualStart !== 0 ||
          read.actualLength !== data.length ||
          !data.equals(readData)
        ) {
          throw new Error(
            `Invalid read (wanted length ${size}, got length ${readData.length})`
          );
        }
      } catch (err) {
        console.error(
          `Failed to upload ${k} (in phase ${phase}): ${err.message}`
        );
      }
      wg.done();
    })();
  }
  await wg;
  await Promise.all(pool.map((conn) => conn.close()));
})();

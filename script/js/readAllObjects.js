const sacli = require("sacli");
const waitGroup = require("@xtjs/lib/js/waitGroup").default;
const readBufferStream = require("@xtjs/lib/js/readBufferStream").default;
const {TurbostoreClient} = require("@turbostore/client");

const key = (no) => `/random/data/${no}`;

sacli.Command.new("readAllObjects")
  .required("concurrency", Number)
  .required("count", Number)
  .required("size", Number)
  .action(async (args) => {
    const data = Buffer.from("DEADBEEF".repeat(Math.ceil(args.size / 8)).slice(0, args.size));
    const pool = Array.from({length: args.concurrency}, () => new TurbostoreClient({host: "127.0.0.1", port: 9001, onSocketError: console.error}));
    const wg = waitGroup();
    const startTime = process.hrtime.bigint();
    for (let no = 0; no < args.count; no++) {
      wg.add();
      const conn = pool[no % pool.length];
      const k = key(no);
      (async () => {
        const read = await conn.readObject(k, 0, 0);
        const readData = await readBufferStream(read.stream);
        if (read.actualStart !== 0 || read.actualLength !== data.length || !data.equals(readData)) {
          throw new Error(`Invalid read (wanted length ${args.size}, got length ${readData.length})`);
        }
        wg.done();
      })();
    }
    await wg;
    const effectiveSec = Number(process.hrtime.bigint() - startTime) / 1e9;
    console.log(`Effective time: ${effectiveSec} seconds`);
    console.log(`Effective processing rate: ${args.count / effectiveSec} per second`);
    console.log(`Effective bandwidth: ${data.length * args.count / 1024 / 1024 / effectiveSec} MiB/s`);
    await Promise.all(pool.map(conn => conn.close()));
  })
  .eval(process.argv.slice(2));

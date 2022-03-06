const sacli = require("sacli");
const waitGroup = require("@xtjs/lib/js/waitGroup").default;
const {TurbostoreClient} = require("@turbostore/client");

const key = (no) => `/random/data/${no}`;

sacli.Command.new("uploadObjects")
  .required("concurrency", Number)
  .required("count", Number)
  .required("size", Number)
  .action(async (args) => {
    const startTime = process.hrtime.bigint();
    const data = Buffer.from("DEADBEEF".repeat(args.size / 8).slice(0, args.size));
    const pool = Array.from({length: args.concurrency}, () => new TurbostoreClient({unixSocketPath: "/tmp/turbostore.sock", onSocketError: console.error}));
    const wg = waitGroup();
    for (let no = 0; no < args.count; no++) {
      wg.add();
      const conn = pool[no % pool.length];
      const k = key(no);
      (async () => {
        let phase;
        try {
          phase = "create";
          const {objectNumber} = await conn.createObject(k, args.size);
          phase = "write";
          await conn.writeObject(k, objectNumber, 0, data);
          phase = "commit";
          await conn.commitObject(k, objectNumber);
        } catch (err) {
          console.error(`Failed to upload ${k} (in phase ${phase}): ${err.message}`);
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

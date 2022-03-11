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
    const data = Buffer.from("DEADBEEF".repeat(Math.ceil(args.size / 8)).slice(0, args.size));
    const poolMgr = Array.from({length: args.concurrency}, () => new TurbostoreClient({host: "127.0.0.1", port: 9000, onSocketError: console.error}));
    const poolWkr = Array.from({length: args.concurrency}, () => new TurbostoreClient({host: "127.0.0.1", port: 9001, onSocketError: console.error}));
    const wg = waitGroup();
    for (let no = 0; no < args.count; no++) {
      wg.add();
      const connMgr = poolMgr[no % poolMgr.length];
      const connWkr = poolWkr[no % poolWkr.length];
      const k = key(no);
      (async () => {
        let phase;
        try {
          phase = "create";
          const {objectNumber} = await connMgr.createObject(k, args.size);
          phase = "write";
          await connWkr.writeObjectWithBuffer(k, objectNumber, 0, data);
          phase = "commit";
          await connMgr.commitObject(k, objectNumber);
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
    await Promise.all(poolMgr.map(conn => conn.close()));
    await Promise.all(poolWkr.map(conn => conn.close()));
  })
  .eval(process.argv.slice(2));

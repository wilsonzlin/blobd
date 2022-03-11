const sacli = require("sacli");
const {TurbostoreClient} = require("@turbostore/client");

sacli.Command.new("uploadObject")
  .required("key", String)
  .required("size", Number)
  .action(async (args) => {
    const data = Buffer.from("DEADBEEF".repeat(Math.ceil(args.size / 8)).slice(0, args.size));
    const connMgr = new TurbostoreClient({host: "127.0.0.1", port: 9000, onSocketError: console.error});
    const connWkr = new TurbostoreClient({host: "127.0.0.1", port: 9001, onSocketError: console.error});
    const {objectNumber} = await connMgr.createObject(args.key, args.size);
    await connWkr.writeObjectWithBuffer(args.key, objectNumber, 0, data);
    await connMgr.commitObject(args.key, objectNumber);
    await connMgr.close();
    await connWkr.close();
  })
  .eval(process.argv.slice(2));

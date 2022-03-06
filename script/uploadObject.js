const sacli = require("sacli");
const {TurbostoreClient} = require("@turbostore/client");

sacli.Command.new("uploadObject")
  .required("key", String)
  .required("size", Number)
  .action(async (args) => {
    const data = Buffer.from("DEADBEEF".repeat(args.size / 8).slice(0, args.size));
    const conn = new TurbostoreClient({unixSocketPath: "/tmp/turbostore.sock", onSocketError: console.error});
    const {objectNumber} = await conn.createObject(args.key, args.size);
    await conn.writeObject(args.key, objectNumber, 0, data);
    await conn.commitObject(args.key, objectNumber);
    await conn.close();
  })
  .eval(process.argv.slice(2));

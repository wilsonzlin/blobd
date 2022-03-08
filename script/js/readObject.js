const sacli = require("sacli");
const {TurbostoreClient} = require("@turbostore/client");

sacli.Command.new("readObject")
  .required("key", String)
  .optional("start", Number)
  .optional("end", Number)
  .action(async (args) => {
    const conn = new TurbostoreClient({unixSocketPath: "/tmp/turbostore.sock", onSocketError: console.error});
    const read = await conn.readObject(args.key, args.start ?? 0, args.end ?? 0);
    console.log(read.data.toString());
    await conn.close();
  })
  .eval(process.argv.slice(2));

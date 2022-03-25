const sacli = require("sacli");
const readBufferStream = require("@xtjs/lib/js/readBufferStream").default;
const {TurbostoreClient} = require("@turbostore/client");

sacli.Command.new("readObject")
  .required("key", String)
  .optional("start", Number)
  .optional("end", Number)
  .action(async (args) => {
    const conn = new TurbostoreClient({host: "127.0.0.1", port: 9001, onSocketError: console.error});
    const read = await conn.readObject(args.key, args.start ?? 0, args.end ?? 0);
    const readData = await readBufferStream(read.stream);
    console.log(readData.toString());
    await conn.close();
  })
  .eval(process.argv.slice(2));

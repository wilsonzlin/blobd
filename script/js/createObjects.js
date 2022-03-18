const sacli = require("sacli");
const {TurbostoreClient} = require("@turbostore/client");

const key = (no) => `/random/data/${no}`;

sacli.Command.new("createObjects")
  .required("size", Number)
  .action(async (args) => {
    const conn = new TurbostoreClient({host: "127.0.0.1", port: 9001});
    let count = 0;
    while (true) {
      try {
        await conn.createObject(key(count), args.size);
        // Increment only on success.
        count++;
      } catch (err) {
        break;
      }
    }
    console.log(`Created ${count} objects (${args.size * count / 1024 / 1024 / 1024} GiB)`);
  })
  .eval(process.argv.slice(2));

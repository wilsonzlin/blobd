const net = require("net");

const KEY = "/path/to/test/file";
const KEY_BYTES = Buffer.from(KEY);

const ARGS_RAW = [
  KEY_BYTES.length,
  ...KEY_BYTES,
];

const conn = net.createConnection({
  path: "/tmp/turbostore.sock",
});

conn.on("data", res => {
  console.log("Received", res);
});

conn.on("end", () => {
  console.log("Ended");
});

conn.write(Buffer.from([
  2,
  ARGS_RAW.length,
  ...ARGS_RAW,
]));

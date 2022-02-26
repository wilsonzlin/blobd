const net = require("net");

const KEY = "/path/to/test/file";
const KEY_BYTES = Buffer.from(KEY);
const SIZE = 36_900_707n;

const buf = Buffer.alloc(8);
buf.writeBigUInt64BE(SIZE);
const SIZE_BYTES = buf.slice();

const ARGS_RAW = [
  KEY_BYTES.length,
  ...KEY_BYTES,
  ...SIZE_BYTES,
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
  1,
  ARGS_RAW.length,
  ...ARGS_RAW,
]));

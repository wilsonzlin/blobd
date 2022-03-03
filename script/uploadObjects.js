const net = require("net");
const fs = require("fs");

const FILES = 1_000_000;
const FILE_SIZE = 1024;

const buf = Buffer.alloc(8);
const encodeU64 = (val) => {
  buf.writeBigUInt64BE(BigInt(val));
  return [...buf];
};

const buildArgs = (method, rawBytes) => Buffer.from([
  method,
  rawBytes.length,
  ...rawBytes,
]);

const create_object = (key, size) => {
  const keyBytes = Buffer.from(key);
  const argsRaw = [
    keyBytes.length,
    ...keyBytes,
    ...encodeU64(size),
  ];
  return buildArgs(1, argsRaw);
};

const write_object = (key, objNo, start) => {
  const keyBytes = Buffer.from(key);
  const argsRaw = [
    keyBytes.length,
    ...keyBytes,
    ...encodeU64(objNo),
    ...encodeU64(start),
  ];
  return buildArgs(4, argsRaw);
};

const commit_object = (key, objNo) => {
  const keyBytes = Buffer.from(key);
  const argsRaw = [
    keyBytes.length,
    ...keyBytes,
    ...encodeU64(objNo),
  ];
  return buildArgs(5, argsRaw);
};

const key = (no) => `/random/data/${no}`;

const conn = net.createConnection({
  path: "/tmp/turbostore.sock",
});

const randomData = fs.readFileSync(`${__dirname}/random-data.bin`);

const uploadFiles = async () => {
  for (let no = 0; no < FILES; no++) {
    const k = key(no);
    conn.write(create_object(k, FILE_SIZE));
    const objNo = await new Promise(resolve => conn.once("data", chunk => {
      if (chunk.length != 9) throw new Error(`Invalid create_object response: ${chunk}`);
      const err = chunk[0];
      if (err != 0) throw new Error(`create_object error: ${err}`);
      const objNo = chunk.readBigUInt64BE(1);
      resolve(objNo);
    }));
    console.log(`create_object => ${objNo}`);
    conn.write(write_object(k, objNo, 0));
    conn.write(randomData.slice(no * FILE_SIZE, (no + 1) * FILE_SIZE));
    await new Promise(resolve => conn.once("data", chunk => {
      if (chunk.length != 1) throw new Error(`Invalid write_object response: ${chunk}`);
      const err = chunk[0];
      if (err != 0) throw new Error(`write_object error: ${err}`);
      resolve();
    }));
    conn.write(commit_object(k, objNo));
    await new Promise(resolve => conn.once("data", chunk => {
      if (chunk.length != 1) throw new Error(`Invalid commit_object response: ${chunk}`);
      const err = chunk[0];
      if (err != 0) throw new Error(`commit_object error: ${err}`);
      resolve();
    }));
  }
  await new Promise(resolve => conn.end(resolve));
};

uploadFiles().then(() => console.log("All done"), console.error);

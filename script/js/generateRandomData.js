const fs = require("fs");
const crypto = require("crypto");

const FILES = 1_000_000;
const FILE_SIZE = 1024;

const out = fs.openSync(`${__dirname}/random-data.bin`, "w");

const WRITE_CHUNK = 64 * 1024;
for (let i = 0; i < FILES * FILE_SIZE; i += WRITE_CHUNK) {
  fs.writeSync(out, crypto.randomBytes(WRITE_CHUNK));
}

fs.closeSync(out);

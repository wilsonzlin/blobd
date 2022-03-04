const net = require("net");
const sacli = require("sacli");
const bigIntToNumber = require("@xtjs/lib/js/bigIntToNumber").default;
const waitGroup = require("@xtjs/lib/js/waitGroup").default;

const buf = Buffer.alloc(8);
const encodeI64 = (val) => {
  buf.writeBigInt64BE(BigInt(val));
  return [...buf];
};
const encodeU64 = (val) => {
  buf.writeBigUInt64BE(BigInt(val));
  return [...buf];
};

const buildArgs = (method, rawBytes) => Buffer.from([
  method,
  rawBytes.length,
  ...rawBytes,
]);

const commit_object = (key, objNo) => {
  const keyBytes = Buffer.from(key);
  const argsRaw = [
    keyBytes.length,
    ...keyBytes,
    ...encodeU64(objNo),
  ];
  return buildArgs(5, argsRaw);
};

const create_object = (key, size) => {
  const keyBytes = Buffer.from(key);
  const argsRaw = [
    keyBytes.length,
    ...keyBytes,
    ...encodeU64(size),
  ];
  return buildArgs(1, argsRaw);
};

const inspect_object = (key) => {
  const keyBytes = Buffer.from(key);
  const argsRaw = [
    keyBytes.length,
    ...keyBytes,
  ];
  return buildArgs(2, argsRaw);
};

const read_object = (key, start, end) => {
  const keyBytes = Buffer.from(key);
  const argsRaw = [
    keyBytes.length,
    ...keyBytes,
    ...encodeI64(start),
    ...encodeI64(end),
  ];
  return buildArgs(3, argsRaw);
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

const awaitRead = async (stream, n) => {
  while (true) {
    const chunk = stream.read(n);
    if (chunk) {
      return chunk;
    }
    await new Promise(resolve => stream.once("readable", resolve));
  }
};

class Connection {
  constructor() {
    this.socket = net.createConnection({
      path: "/tmp/turbostore.sock",
    });
    this.onAvailable = Promise.resolve();
  }

  _enqueue(fn) {
    return new Promise((resolve, reject) => {
      this.onAvailable = this.onAvailable.then(() => fn().then(resolve, reject));
    });
  }

  commitObject(key, objNo) {
    return this._enqueue(async () => {
      this.socket.write(commit_object(key, objNo));
      const chunk = await awaitRead(this.socket, 1);
      if (chunk.length != 1) throw new Error(`Invalid commit_object response: ${chunk}`);
      const err = chunk[0];
      if (err != 0) throw new Error(`commit_object error: ${err}`);
    });
  }

  createObject(key, size) {
    return this._enqueue(async () => {
      this.socket.write(create_object(key, size));
      const chunk = await awaitRead(this.socket, 9);
      if (chunk.length != 9) throw new Error(`Invalid create_object response: ${chunk}`);
      const err = chunk[0];
      if (err != 0) throw new Error(`create_object error: ${err}`);
      const objNo = bigIntToNumber(chunk.readBigUInt64BE(1));
      return {
        objectNumber: objNo,
      };
    });
  }

  inspectObject(key) {
    return this._enqueue(async () => {
      this.socket.write(inspect_object(key));
      const chunk = await awaitRead(this.socket, 10);
      if (chunk.length != 10) throw new Error(`Invalid inspect_object response: ${chunk}`);
      const err = chunk[0];
      if (err != 0) throw new Error(`inspect_object error: ${err}`);
      const state = chunk[1];
      const size = bigIntToNumber(chunk.readBigUInt64BE(2));
      return {
        state,
        size,
      };
    });
  }

  readObject(key, start, end) {
    return this._enqueue(async () => {
      this.socket.write(read_object(key, start, end));
      const chunk = await awaitRead(this.socket, 17);
      if (chunk.length != 17) throw new Error(`Invalid read_object response: ${chunk}`);
      const err = chunk[0];
      if (err != 0) throw new Error(`read_object error: ${err}`);
      const actualStart = bigIntToNumber(chunk.readBigUInt64BE(1));
      const actualLength = bigIntToNumber(chunk.readBigUInt64BE(9));
      const data = await awaitRead(this.socket, actualLength);
      return {
        data,
        actualStart,
        actualLength,
      };
    });
  }

  writeObject(key, objNo, start, data) {
    return this._enqueue(async () => {
      this.socket.write(write_object(key, objNo, start));
      this.socket.write(data);
      const chunk = await awaitRead(this.socket, 1);
      if (chunk.length != 1) throw new Error(`Invalid write_object response: ${chunk}`);
      const err = chunk[0];
      if (err != 0) throw new Error(`write_object error: ${err}`);
    });
  }
}

const key = (no) => `/random/data/${no}`;

sacli.Command.new("uploadObjects")
  .required("concurrency", Number)
  .required("count", Number)
  .required("size", Number)
  .action(async (args) => {
    const data = Buffer.from("DEADBEEF".repeat(args.size / 8).slice(0, args.size));
    const pool = Array.from({length: args.concurrency}, () => new Connection());
    const wg = waitGroup();
    for (let no = 0; no < args.count; no++) {
      wg.add();
      const conn = pool[no % pool.length];
      const k = key(no);
      (async () => {
        const {objectNumber} = await conn.createObject(k, args.size);
        await conn.writeObject(k, objectNumber, 0, data);
        await conn.commitObject(k, objectNumber);
        const inspection = await conn.inspectObject(k);
        if (inspection.size !== data.length) {
          throw new Error(`Inspection resulted in size ${inspection.size}`);
        }
        const read = await conn.readObject(k, 0, 0);
        if (read.actualStart !== 0 || read.actualLength !== data.length || !data.equals(read.data)) {
          throw new Error(`Invalid read`);
        }
      })();
    }
    await wg;
    await new Promise(resolve => conn.end(resolve));
  })
  .eval(process.argv.slice(2));

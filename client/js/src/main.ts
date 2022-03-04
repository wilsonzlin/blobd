import net from "net";
import bigIntToNumber from "@xtjs/lib/js/bigIntToNumber";
import { Readable } from "stream";

const buf = Buffer.alloc(8);
const encodeI64 = (val: number) => {
  buf.writeBigInt64BE(BigInt(val));
  return [...buf];
};
const encodeU64 = (val: number) => {
  buf.writeBigUInt64BE(BigInt(val));
  return [...buf];
};

const buildArgs = (method: number, rawBytes: number[]) =>
  Buffer.from([method, rawBytes.length, ...rawBytes]);

const commit_object = (key: string, objNo: number) => {
  const keyBytes = Buffer.from(key);
  const argsRaw = [keyBytes.length, ...keyBytes, ...encodeU64(objNo)];
  return buildArgs(5, argsRaw);
};

const create_object = (key: string, size: number) => {
  const keyBytes = Buffer.from(key);
  const argsRaw = [keyBytes.length, ...keyBytes, ...encodeU64(size)];
  return buildArgs(1, argsRaw);
};

const inspect_object = (key: string) => {
  const keyBytes = Buffer.from(key);
  const argsRaw = [keyBytes.length, ...keyBytes];
  return buildArgs(2, argsRaw);
};

const read_object = (key: string, start: number, end: number) => {
  const keyBytes = Buffer.from(key);
  const argsRaw = [
    keyBytes.length,
    ...keyBytes,
    ...encodeI64(start),
    ...encodeI64(end),
  ];
  return buildArgs(3, argsRaw);
};

const write_object = (key: string, objNo: number, start: number) => {
  const keyBytes = Buffer.from(key);
  const argsRaw = [
    keyBytes.length,
    ...keyBytes,
    ...encodeU64(objNo),
    ...encodeU64(start),
  ];
  return buildArgs(4, argsRaw);
};

const read = async (stream: Readable, n?: number) => {
  while (true) {
    const chunk = stream.read(n);
    if (chunk) {
      return chunk;
    }
    await new Promise((resolve) => stream.once("readable", resolve));
  }
};

export enum TurbostoreErrorCode {
  OK = 0,
  NOT_ENOUGH_ARGS = 1,
  KEY_TOO_LONG = 2,
  TOO_MANY_ARGS = 3,
  NOT_FOUND = 4,
  INVALID_START = 5,
  INVALID_END = 6,
}

export class TurbostoreError extends Error {
  constructor(
    readonly requestType: string,
    readonly errorCode: TurbostoreErrorCode
  ) {
    super(
      `${requestType} request failed with error ${TurbostoreErrorCode[errorCode]}`
    );
  }
}

export class TurbostoreClient {
  private readonly socket: net.Socket;
  private onAvailable: Promise<unknown>;

  constructor() {
    this.socket = net.createConnection({
      path: "/tmp/turbostore.sock",
    });
    this.onAvailable = Promise.resolve();
  }

  _enqueue<T>(fn: () => Promise<T>) {
    return new Promise<T>((resolve, reject) => {
      this.onAvailable = this.onAvailable.then(() =>
        fn().then(resolve, reject)
      );
    });
  }

  close() {
    return new Promise<void>((resolve) => this.socket.end(resolve));
  }

  commitObject(key: string, objNo: number) {
    return this._enqueue(async () => {
      this.socket.write(commit_object(key, objNo));
      const chunk = await read(this.socket, 1);
      if (chunk.length != 1)
        throw new Error(`Invalid commit_object response: ${chunk}`);
      const err = chunk[0];
      if (err != 0) throw new TurbostoreError("commit_object", err);
    });
  }

  createObject(key: string, size: number) {
    return this._enqueue(async () => {
      this.socket.write(create_object(key, size));
      const chunk = await read(this.socket, 9);
      if (chunk.length != 9)
        throw new Error(`Invalid create_object response: ${chunk}`);
      const err = chunk[0];
      if (err != 0) throw new TurbostoreError("create_object", err);
      const objNo = bigIntToNumber(chunk.readBigUInt64BE(1));
      return {
        objectNumber: objNo,
      };
    });
  }

  inspectObject(key: string) {
    return this._enqueue(async () => {
      this.socket.write(inspect_object(key));
      const chunk = await read(this.socket, 10);
      if (chunk.length != 10)
        throw new Error(`Invalid inspect_object response: ${chunk}`);
      const err = chunk[0];
      if (err != 0) throw new TurbostoreError("inspect_object", err);
      const state = chunk[1];
      const size = bigIntToNumber(chunk.readBigUInt64BE(2));
      return {
        state,
        size,
      };
    });
  }

  readObject(key: string, start: number, end: number) {
    return this._enqueue(async () => {
      this.socket.write(read_object(key, start, end));
      const chunk = await read(this.socket, 17);
      if (chunk.length != 17)
        throw new Error(`Invalid read_object response: ${chunk}`);
      const err = chunk[0];
      if (err != 0) throw new TurbostoreError("read_object", err);
      const actualStart = bigIntToNumber(chunk.readBigUInt64BE(1));
      const actualLength = bigIntToNumber(chunk.readBigUInt64BE(9));
      const data = [];
      let drainedLength = 0;
      while (drainedLength < actualLength) {
        // Node.js will stall if we try to read in one go, because it'll refuse to buffer too much data, so we must drain in chunks.
        const chunk = await read(this.socket);
        drainedLength += chunk.length;
        data.push(chunk);
      }
      return {
        data: Buffer.concat(data, drainedLength),
        actualStart,
        actualLength,
      };
    });
  }

  writeObject(
    key: string,
    objNo: number,
    start: number,
    data: Uint8Array | Readable
  ) {
    return this._enqueue(async () => {
      this.socket.write(write_object(key, objNo, start));
      if (data instanceof Uint8Array) {
        this.socket.write(data);
      } else {
        // Keep writing until the server responds.
        while (!this.socket.readableLength) {
          let chunk;
          if (
            data.readable &&
            this.socket.writable &&
            (this.socket as any).writableNeedDrain &&
            (chunk = data.read())
          ) {
            // TODO We need some framing protocol in case the server decides to fail the request early and then proceed to read the rest of the chunk as the start of another request.
            this.socket.write(chunk);
          }
        }
      }
      const chunk = await read(this.socket, 1);
      if (chunk.length != 1)
        throw new Error(`Invalid write_object response: ${chunk}`);
      const err = chunk[0];
      if (err != 0) throw new TurbostoreError("write_object", err);
    });
  }
}

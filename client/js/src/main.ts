import net from "net";
import bigIntToNumber from "@xtjs/lib/js/bigIntToNumber";
import { Readable, Writable } from "stream";
import Mutex from "@xtjs/lib/js/mutex";
import util from "util";
import assertState from "@xtjs/lib/js/assertState";

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
  Buffer.from([
    rawBytes.length,
    method,
    ...rawBytes,
    ...Array(255 - rawBytes.length - 2).fill(0),
  ]);

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

const delete_object = (key: string, objNo: number) => {
  const keyBytes = Buffer.from(key);
  const argsRaw = [keyBytes.length, ...keyBytes, ...encodeU64(objNo)];
  return buildArgs(6, argsRaw);
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
    // Check after .read() as state could technically synchronously change with .read() call, and if it returned null because it's ended (not because it's not readable yet), then we get stuck. Check after `chunk` as it could be nonreadable after final read().
    if (!stream.readable) {
      throw new Error("TurbostoreClient socket stream is not readable");
    }
    await new Promise<void>((resolve, reject) => {
      const handler = (error?: Error) => {
        // "readable" is also emitted on end.
        stream.off("error", handler).off("readable", handler);
        error ? reject(error) : resolve();
      };
      stream.on("error", handler).on("readable", handler);
    });
  }
};

const assertValidResponse = (method: string, res: Buffer, len: number) => {
  if (res.length !== len) {
    throw new Error(
      `Invalid ${method} response: ${util.inspect(res, {
        colors: false,
        depth: null,
        showHidden: false,
        compact: true,
      })}`
    );
  }
  if (res[0] !== 0) {
    throw new TurbostoreError(method, res[0]);
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
  private mutex = new Mutex();

  constructor({
    host,
    // Must be provided to avoid crashing Node.js on connection errors while idle in the background.
    onSocketError,
    port,
    unixSocketPath,
  }: {
    host?: string;
    onSocketError: (error: Error) => void;
    port?: number;
    unixSocketPath?: string;
  }) {
    this.socket = net.createConnection({
      host,
      path: unixSocketPath,
      port: port as any,
    });
    this.socket.on("error", onSocketError);
  }

  close() {
    return new Promise<void>((resolve) => this.socket.end(resolve));
  }

  async commitObject(key: string, objNo: number) {
    const l = await this.mutex.lock();
    try {
      this.socket.write(commit_object(key, objNo));
      const chunk = await read(this.socket, 1);
      assertValidResponse("commit_object", chunk, 1);
    } finally {
      l.unlock();
    }
  }

  async createObject(key: string, size: number) {
    const l = await this.mutex.lock();
    try {
      this.socket.write(create_object(key, size));
      const chunk = await read(this.socket, 9);
      assertValidResponse("create_object", chunk, 9);
      return {
        objectNumber: bigIntToNumber(chunk.readBigUInt64BE(1)),
      };
    } finally {
      l.unlock();
    }
  }

  async deleteObject(key: string, objNo?: number) {
    const l = await this.mutex.lock();
    try {
      this.socket.write(delete_object(key, objNo ?? 0));
      const chunk = await read(this.socket, 1);
      assertValidResponse("delete_object", chunk, 1);
    } finally {
      l.unlock();
    }
  }

  async inspectObject(key: string) {
    const l = await this.mutex.lock();
    try {
      this.socket.write(inspect_object(key));
      const chunk = await read(this.socket, 10);
      assertValidResponse("inspect_object", chunk, 10);
      return {
        state: chunk[1],
        size: bigIntToNumber(chunk.readBigUInt64BE(2)),
      };
    } finally {
      l.unlock();
    }
  }

  async readObject(key: string, start: number, end: number) {
    const l = await this.mutex.lock();
    const { socket } = this;
    let actualStart: number;
    let actualLength: number;
    let objectSize: number;
    try {
      socket.write(read_object(key, start, end));
      const chunk = await read(socket, 25);
      assertValidResponse("read_object", chunk, 25);
      actualStart = bigIntToNumber(chunk.readBigUInt64BE(1));
      actualLength = bigIntToNumber(chunk.readBigUInt64BE(9));
      objectSize = bigIntToNumber(chunk.readBigUInt64BE(17));
    } catch (err) {
      l.unlock();
      throw err;
    }

    let pushedLen = 0;
    let canPush = true;
    const maybePush = () => {
      let chunk;
      while (
        socket.readable &&
        stream.readable &&
        canPush &&
        (chunk = socket.read())
      ) {
        canPush = stream.push(chunk);
        pushedLen += chunk.length;
        assertState(
          pushedLen <= actualLength,
          `read ${pushedLen} of ${actualLength} bytes`
        );
        if (pushedLen == actualLength) {
          stream.push(null);
          cleanUp();
        }
      }
      if (!socket.readable) {
        // The socket should never end or close, even after all object data has been provided (one connection handles infinite requests).
        // This handles socket "end", "error", "close".
        stream.destroy(
          new Error("TurbostoreClient socket is no longer readable")
        );
        cleanUp();
      }
    };
    const cleanUp = () =>
      stream.off("close", maybePush).off("readable", maybePush);
    socket.on("close", maybePush).on("readable", maybePush);
    const stream = new Readable({
      destroy(err, cb) {
        if (pushedLen < actualLength) {
          socket.destroy(new Error("read_object downstream was destroyed"));
        }
        l.unlock();
        cb(err);
      },
      read(_n) {
        canPush = true;
        maybePush();
      },
    });
    maybePush();
    return {
      stream,
      actualStart,
      actualLength,
      objectSize,
    };
  }

  async writeObjectWithBuffer(
    key: string,
    objNo: number,
    start: number,
    data: Uint8Array
  ) {
    const l = await this.mutex.lock();
    try {
      this.socket.write(write_object(key, objNo, start));
      this.socket.write(data);
      const chunk = await read(this.socket, 1);
      assertValidResponse("write_object", chunk, 1);
    } finally {
      l.unlock();
    }
  }

  async writeObjectWithStream(key: string, objNo: number, start: number) {
    const l = await this.mutex.lock();
    const { socket } = this;
    try {
      socket.write(write_object(key, objNo, start));
    } catch (err) {
      l.unlock();
      throw err;
    }
    // We don't need a "close" handler on `socket` as both `socket.write` and `read(socket, 1)` will fail if it's closed.
    let readResponse = false;
    const stream = new Writable({
      destroy(err, cb) {
        if (!readResponse) {
          socket.destroy(new Error("write_object downstream was destroyed"));
        }
        l.unlock();
        cb(err);
      },
      final(cb) {
        // TODO Handle case where not enough bytes were written.
        read(socket, 1)
          .then((chunk) => {
            readResponse = true;
            assertValidResponse("write_object", chunk, 1);
            cb();
          })
          .catch(cb);
      },
      write(chunk, encoding, cb) {
        // TODO Handle case where too many bytes are written.
        socket.write(chunk, encoding, cb);
      },
    });
    return stream;
  }
}

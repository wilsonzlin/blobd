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
      if (chunk.length != 1) {
        throw new Error(`Invalid commit_object response: ${chunk}`);
      }
      const err = chunk[0];
      if (err != 0) throw new TurbostoreError("commit_object", err);
    });
  }

  createObject(key: string, size: number) {
    return this._enqueue(async () => {
      this.socket.write(create_object(key, size));
      const chunk = await read(this.socket, 9);
      if (chunk.length != 9) {
        throw new Error(`Invalid create_object response: ${chunk}`);
      }
      const err = chunk[0];
      if (err != 0) throw new TurbostoreError("create_object", err);
      const objNo = bigIntToNumber(chunk.readBigUInt64BE(1));
      return {
        objectNumber: objNo,
      };
    });
  }

  deleteObject(key: string, objNo?: number) {
    return this._enqueue(async () => {
      this.socket.write(delete_object(key, objNo ?? 0));
      const chunk = await read(this.socket, 1);
      if (chunk.length != 1) {
        throw new Error(`Invalid delete_object response: ${chunk}`);
      }
      const err = chunk[0];
      if (err != 0) throw new TurbostoreError("delete_object", err);
    });
  }

  inspectObject(key: string) {
    return this._enqueue(async () => {
      this.socket.write(inspect_object(key));
      const chunk = await read(this.socket, 10);
      if (chunk.length != 10) {
        throw new Error(`Invalid inspect_object response: ${chunk}`);
      }
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
      if (chunk.length != 17) {
        throw new Error(`Invalid read_object response: ${chunk}`);
      }
      const err = chunk[0];
      if (err != 0) throw new TurbostoreError("read_object", err);
      const actualStart = bigIntToNumber(chunk.readBigUInt64BE(1));
      const actualLength = bigIntToNumber(chunk.readBigUInt64BE(9));

      let pushedLen = 0;
      let canPush = true;
      const maybePush = () => {
        let chunk;
        while (
          this.socket.readable &&
          stream.readable &&
          canPush &&
          (chunk = this.socket.read())
        ) {
          canPush = stream.push(chunk);
          if ((pushedLen += chunk.length) == actualLength) {
            stream.push(null);
            stream.off("close", upstreamCloseHandler);
          }
        }
      };
      const { socket } = this;
      const upstreamCloseHandler = () =>
        stream.destroy(new Error("TurbostoreClient socket was destroyed"));
      socket.on("close", upstreamCloseHandler);
      const stream = new Readable({
        destroy(err, cb) {
          // TODO We need some protocol that allows us to cancel a request.
          socket.destroy(new Error("read_object downstream was destroyed"));
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
        while (
          // Keep writing until the server responds.
          !this.socket.readableLength &&
          // The server may have disconnected.
          this.socket.writable &&
          // The source may have ended or been destroyed.
          data.readable
        ) {
          if ((this.socket as any).writableNeedDrain) {
            await new Promise<void>((resolve, reject) => {
              const handler = (error?: Error) => {
                this.socket.off("error", handler).off("drain", handler);
                error ? reject(error) : resolve();
              };
              this.socket.on("error", handler).on("drain", handler);
            });
          }
          if (!data.readableLength) {
            await new Promise<void>((resolve, reject) => {
              const handler = (error?: Error) => {
                // "readable" is also emitted on end.
                data.off("error", handler).off("readable", handler);
                error ? reject(error) : resolve();
              };
              data.on("error", handler).on("readable", handler);
            });
          }
          let chunk;
          if ((chunk = data.read())) {
            this.socket.write(chunk);
          }
        }
      }
      const chunk = await read(this.socket, 1);
      if (chunk.length != 1) {
        throw new Error(`Invalid write_object response: ${chunk}`);
      }
      const err = chunk[0];
      if (err != 0) throw new TurbostoreError("write_object", err);
    });
  }
}

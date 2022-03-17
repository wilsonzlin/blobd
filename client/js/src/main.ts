import net from "net";
import bigIntToNumber from "@xtjs/lib/js/bigIntToNumber";
import { Readable, Writable } from "stream";
import Pool from "@xtjs/lib/js/Pool";
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
    method,
    ...rawBytes,
    ...Array(255 - rawBytes.length - 1).fill(0),
  ]);

const commit_object = (inodeDevOffset: number, objNo: number) => {
  const argsRaw = [...encodeU64(inodeDevOffset), ...encodeU64(objNo)];
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

const write_object = (
  inodeDevOffset: number,
  objNo: number,
  start: number,
  len: number
) => {
  const argsRaw = [
    ...encodeU64(inodeDevOffset),
    ...encodeU64(objNo),
    ...encodeU64(start),
    ...encodeU64(len),
  ];
  return buildArgs(4, argsRaw);
};

const read = async (stream: Readable, n?: number) => {
  while (true) {
    const chunk = stream.read(n);
    if (chunk) {
      // TODO Check length and destroy stream if mismatch?
      return chunk;
    }
    // Check after .read() as state could technically synchronously change with .read() call, and if it returned null because it's ended (not because it's not readable yet), then we get stuck. Check after `chunk` as it could be nonreadable after final read().
    if (!stream.readable) {
      throw new Error("Turbostore connection stream is not readable");
    }
    await new Promise<void>((resolve, reject) => {
      const handler = (error?: Error) => {
        stream.off("error", handler).off("readable", handler);
        error ? reject(error) : resolve();
      };
      // "readable" is also emitted on end.
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

export class TurbostoreReadObjectStream extends Readable {
  private pushedLen = 0;
  private canPush = true;

  constructor(
    private readonly socket: net.Socket,
    readonly actualStart: number,
    readonly actualLength: number,
    readonly objectSize: number
  ) {
    super({
      autoDestroy: true,
      emitClose: true,
    });
    // When makeRequest acquired the client, it attached a "close" listener on `socket` and will destroy this TurbostoreReadObjectStream if `socket` closes, so we don't need to handle that here.
    socket.on("readable", this.maybePush);
    this.maybePush();
  }

  private cleanUp = () => {
    this.socket.off("readable", this.maybePush);
  };

  private maybePush = () => {
    let chunk;
    while (
      this.socket.readable &&
      this.readable &&
      this.canPush &&
      (chunk = this.socket.read())
    ) {
      this.canPush = this.push(chunk);
      this.pushedLen += chunk.length;
      assertState(
        this.pushedLen <= this.actualLength,
        `read ${this.pushedLen} of ${this.actualLength} bytes`
      );
      if (this.pushedLen == this.actualLength) {
        this.push(null);
        this.cleanUp();
      }
    }
  };

  override _destroy(
    error: Error | null,
    callback: (error?: Error | null) => void
  ): void {
    this.cleanUp();
    if (this.pushedLen < this.actualLength) {
      this.socket.destroy(new Error("read_object downstream was destroyed"));
    }
    callback(error);
  }

  override _read(_size: number): void {
    this.canPush = true;
    this.maybePush();
  }
}

export class TurbostoreWriteObjectStream extends Writable {
  private readResponse = false;

  constructor(private readonly socket: net.Socket) {
    super({
      autoDestroy: true,
      emitClose: true,
    });
    // We don't need a "close" handler on `socket` as both `socket.write` and `read(socket, 1)` will fail if it's closed.
  }

  override _destroy(
    error: Error | null,
    callback: (error?: Error | null) => void
  ): void {
    if (!this.readResponse) {
      this.socket.destroy(new Error("write_object downstream was destroyed"));
    }
    callback(error);
  }

  override _final(callback: (error?: Error | null) => void): void {
    // TODO Handle case where not enough bytes were written.
    read(this.socket, 1)
      .then((chunk) => {
        this.readResponse = true;
        assertValidResponse("write_object", chunk, 1);
        callback();
      })
      .catch(callback);
  }

  override _write(
    chunk: any,
    encoding: BufferEncoding,
    callback: (error?: Error | null) => void
  ): void {
    // TODO Handle case where too many bytes are written.
    this.socket.write(chunk, encoding, callback);
  }
}

export class TurbostoreClient {
  private readonly clients = new Pool<net.Socket>();

  constructor(
    private readonly clientOpt: {
      host?: string;
      port?: number;
      unixSocketPath?: string;
    }
  ) {}

  private getClient() {
    let client = this.clients.getAvailable();
    if (!client) {
      client = net
        .createConnection({
          host: this.clientOpt.host,
          path: this.clientOpt.unixSocketPath,
          port: this.clientOpt.port as any,
        })
        // Suppress any errors. If connection drops while idle, next request will notice.
        .on("error", () => void 0);
      this.clients.registerNewItem(client);
    }
    return client;
  }

  private async makeRequest<T>(fn: (client: net.Socket) => Promise<T>) {
    const client = this.getClient();
    this.clients.makeUnavailable(client);
    let res: T | undefined;
    let err: Error | undefined;
    try {
      res = await fn(client);
    } catch (e) {
      err = e;
    }
    const cleanUp = () => {
      if (client.readable) {
        this.clients.makeAvailable(client);
      } else {
        this.clients.deregisterItem(client);
      }
    };
    if (res instanceof Readable || res instanceof Writable) {
      const stream = res;
      // The socket should never end or close, even after all object data has been provided (one connection handles infinite requests).
      // This handles socket "end", "error", "close".
      stream.once("close", () => {
        stream.destroy(new Error("Turbostore connection has closed"));
        cleanUp();
      });
    } else {
      cleanUp();
    }
    if (err) {
      throw err;
    }
    return res!;
  }

  async commitObject(inodeDevOffset: number, objNo: number) {
    return this.makeRequest(async (socket) => {
      socket.write(commit_object(inodeDevOffset, objNo));
      const chunk = await read(socket, 1);
      assertValidResponse("commit_object", chunk, 1);
    });
  }

  async createObject(key: string, size: number) {
    return this.makeRequest(async (socket) => {
      socket.write(create_object(key, size));
      const chunk = await read(socket, 17);
      assertValidResponse("create_object", chunk, 17);
      return {
        inodeDeviceOffset: bigIntToNumber(chunk.readBigUInt64BE(1)),
        objectNumber: bigIntToNumber(chunk.readBigUInt64BE(9)),
      };
    });
  }

  async deleteObject(key: string, objNo?: number) {
    return this.makeRequest(async (socket) => {
      socket.write(delete_object(key, objNo ?? 0));
      const chunk = await read(socket, 1);
      assertValidResponse("delete_object", chunk, 1);
    });
  }

  async inspectObject(key: string) {
    return this.makeRequest(async (socket) => {
      socket.write(inspect_object(key));
      const chunk = await read(socket, 10);
      assertValidResponse("inspect_object", chunk, 10);
      return {
        state: chunk[1],
        size: bigIntToNumber(chunk.readBigUInt64BE(2)),
      };
    });
  }

  async readObject(key: string, start: number, end: number) {
    return this.makeRequest(async (socket) => {
      socket.write(read_object(key, start, end));
      const chunk = await read(socket, 25);
      assertValidResponse("read_object", chunk, 25);
      const actualStart = bigIntToNumber(chunk.readBigUInt64BE(1));
      const actualLength = bigIntToNumber(chunk.readBigUInt64BE(9));
      const objectSize = bigIntToNumber(chunk.readBigUInt64BE(17));
      return new TurbostoreReadObjectStream(
        socket,
        actualStart,
        actualLength,
        objectSize
      );
    });
  }

  async writeObjectWithBuffer(
    inodeDevOffset: number,
    objNo: number,
    start: number,
    data: Uint8Array
  ) {
    return this.makeRequest(async (socket) => {
      socket.write(write_object(inodeDevOffset, objNo, start, data.length));
      socket.write(data);
      const chunk = await read(socket, 1);
      assertValidResponse("write_object", chunk, 1);
    });
  }

  async writeObjectWithStream(
    inodeDevOffset: number,
    objNo: number,
    start: number,
    len: number
  ) {
    return this.makeRequest(async (socket) => {
      socket.write(write_object(inodeDevOffset, objNo, start, len));
      return new TurbostoreWriteObjectStream(socket);
    });
  }
}

import { AuthToken, AuthTokenAction, BlobdTokens } from "./tokens";
import {
  BatchCreateObjectEntry,
  BatchCreatedObjects,
  CreatedObject,
  InspectedObject,
  WrittenObjectPart,
} from "./types";

function now(): number {
  return Math.floor(Date.now() / 1000);
}

function encodeU16BE(val: number): Uint8Array {
  const buf = new Uint8Array(2);
  new DataView(buf.buffer).setUint16(0, val, false);
  return buf;
}

function encodeU40BE(val: number): Uint8Array {
  const buf = new Uint8Array(5);
  const view = new DataView(buf.buffer);
  // Store as 40-bit big-endian (5 bytes)
  const bigVal = BigInt(val);
  view.setUint8(0, Number((bigVal >> 32n) & 0xffn));
  view.setUint32(1, Number(bigVal & 0xffffffffn), false);
  return buf;
}

export class BlobdClient {
  private readonly tokens: BlobdTokens;

  constructor(private readonly endpoint: string, tokenSecret: Buffer) {
    // Remove trailing slash if present
    this.endpoint = endpoint.replace(/\/$/, "");
    this.tokens = new BlobdTokens(tokenSecret);
  }

  private buildUrl(key: string): string {
    // Split key by '/' and percent-encode each segment
    const segments = key
      .split("/")
      .map((segment) => encodeURIComponent(segment));
    return `${this.endpoint}/${segments.join("/")}`;
  }

  generateTokenQueryParam(
    action: AuthTokenAction,
    expiresInSeconds: number
  ): [string, string] {
    const t = AuthToken.new(this.tokens, action, now() + expiresInSeconds);
    return ["t", t];
  }

  generatePresignedUrl(
    key: string,
    action: AuthTokenAction,
    expiresInSeconds: number
  ): string {
    const url = new URL(this.buildUrl(key));
    const [k, v] = this.generateTokenQueryParam(action, expiresInSeconds);
    url.searchParams.append(k, v);
    return url.toString();
  }

  async createObject(key: string, size: number): Promise<CreatedObject> {
    const keyBytes = new TextEncoder().encode(key);
    const url = new URL(this.buildUrl(key));
    url.searchParams.append("size", size.toString());
    const [tk, tv] = this.generateTokenQueryParam(
      { type: "CreateObject", key: keyBytes, size },
      300
    );
    url.searchParams.append(tk, tv);

    const response = await fetch(url.toString(), {
      method: "POST",
    });

    if (!response.ok) {
      throw new Error(
        `Failed to create object: ${response.status} ${response.statusText}`
      );
    }

    const uploadToken = response.headers.get("x-blobd-upload-token");
    if (!uploadToken) {
      throw new Error("Missing x-blobd-upload-token header in response");
    }

    return { uploadToken };
  }

  async writeObject(
    key: string,
    creation: CreatedObject,
    offset: number,
    data: Uint8Array | ReadableStream<Uint8Array>
  ): Promise<WrittenObjectPart> {
    const keyBytes = new TextEncoder().encode(key);
    const url = new URL(this.buildUrl(key));
    url.searchParams.append("offset", offset.toString());
    url.searchParams.append("upload_token", creation.uploadToken);
    const [tk, tv] = this.generateTokenQueryParam(
      { type: "WriteObject", key: keyBytes },
      300
    );
    url.searchParams.append(tk, tv);

    const contentLength = data instanceof Uint8Array ? data.length : undefined;

    const headers: HeadersInit = {};
    if (contentLength !== undefined) {
      headers["Content-Length"] = contentLength.toString();
    }

    const response = await fetch(url.toString(), {
      method: "PATCH",
      body: data,
      headers,
      duplex: "half",
    } as RequestInit);

    if (!response.ok) {
      throw new Error(
        `Failed to write object: ${response.status} ${response.statusText}`
      );
    }

    const writeReceipt = response.headers.get("x-blobd-write-receipt");
    if (!writeReceipt) {
      throw new Error("Missing x-blobd-write-receipt header in response");
    }

    return { writeReceipt };
  }

  async commitObject(
    key: string,
    creation: CreatedObject,
    writeReceipts: string[]
  ): Promise<void> {
    const keyBytes = new TextEncoder().encode(key);
    const url = new URL(this.buildUrl(key));
    url.searchParams.append("upload_token", creation.uploadToken);
    url.searchParams.append("write_receipts", writeReceipts.join(","));
    const [tk, tv] = this.generateTokenQueryParam(
      { type: "CommitObject", key: keyBytes },
      300
    );
    url.searchParams.append(tk, tv);

    const response = await fetch(url.toString(), {
      method: "PUT",
    });

    if (!response.ok) {
      throw new Error(
        `Failed to commit object: ${response.status} ${response.statusText}`
      );
    }
  }

  async readObject(
    key: string,
    start: number,
    end?: number
  ): Promise<ReadableStream<Uint8Array>> {
    const keyBytes = new TextEncoder().encode(key);
    const url = new URL(this.buildUrl(key));
    const [tk, tv] = this.generateTokenQueryParam(
      { type: "ReadObject", key: keyBytes },
      300
    );
    url.searchParams.append(tk, tv);

    const rangeEnd = end !== undefined ? end - 1 : ""; // HTTP Range is inclusive
    const response = await fetch(url.toString(), {
      method: "GET",
      headers: {
        Range: `bytes=${start}-${rangeEnd}`,
      },
    });

    if (!response.ok) {
      throw new Error(
        `Failed to read object: ${response.status} ${response.statusText}`
      );
    }

    if (!response.body) {
      throw new Error("Response body is null");
    }

    return response.body;
  }

  async inspectObject(key: string): Promise<InspectedObject> {
    const keyBytes = new TextEncoder().encode(key);
    const url = new URL(this.buildUrl(key));
    const [tk, tv] = this.generateTokenQueryParam(
      { type: "InspectObject", key: keyBytes },
      300
    );
    url.searchParams.append(tk, tv);

    const response = await fetch(url.toString(), {
      method: "HEAD",
    });

    if (!response.ok) {
      throw new Error(
        `Failed to inspect object: ${response.status} ${response.statusText}`
      );
    }

    const contentLength = response.headers.get("content-length");
    const objectId = response.headers.get("x-blobd-object-id");

    if (!contentLength || !objectId) {
      throw new Error("Missing required headers in response");
    }

    return {
      objectId: parseInt(objectId, 10),
      contentLength: parseInt(contentLength, 10),
    };
  }

  async deleteObject(key: string): Promise<void> {
    const keyBytes = new TextEncoder().encode(key);
    const url = new URL(this.buildUrl(key));
    const [tk, tv] = this.generateTokenQueryParam(
      { type: "DeleteObject", key: keyBytes },
      300
    );
    url.searchParams.append(tk, tv);

    const response = await fetch(url.toString(), {
      method: "DELETE",
    });

    if (!response.ok) {
      throw new Error(
        `Failed to delete object: ${response.status} ${response.statusText}`
      );
    }
  }

  async batchCreateObjects(
    objects:
      | AsyncIterable<BatchCreateObjectEntry>
      | Iterable<BatchCreateObjectEntry>,
    transferByteCounter?: (bytes: number) => void
  ): Promise<BatchCreatedObjects> {
    const url = new URL(this.endpoint);
    const [tk, tv] = this.generateTokenQueryParam(
      { type: "BatchCreateObjects" },
      300
    );
    url.searchParams.append(tk, tv);

    // Create a readable stream that encodes objects in the binary format
    const stream = new ReadableStream<Uint8Array>({
      async start(controller) {
        try {
          for await (const entry of objects) {
            // Encode key length (u16 BE)
            const keyBytes = new TextEncoder().encode(entry.key);
            controller.enqueue(encodeU16BE(keyBytes.length));

            // Encode key
            controller.enqueue(keyBytes);

            // Encode size (u40 BE)
            controller.enqueue(encodeU40BE(entry.size));

            // Encode data
            if (entry.data instanceof Uint8Array) {
              transferByteCounter?.(entry.data.length);
              controller.enqueue(entry.data);
            } else {
              // Stream data
              const reader = entry.data.getReader();
              try {
                while (true) {
                  const { done, value } = await reader.read();
                  if (done) break;
                  transferByteCounter?.(value.length);
                  controller.enqueue(value);
                }
              } finally {
                reader.releaseLock();
              }
            }
          }
          controller.close();
        } catch (error) {
          controller.error(error);
        }
      },
    });

    const response = await fetch(url.toString(), {
      method: "POST",
      body: stream,
      duplex: "half",
    } as RequestInit);

    if (!response.ok) {
      throw new Error(
        `Failed to batch create objects: ${response.status} ${response.statusText}`
      );
    }

    const countHeader = response.headers.get("x-blobd-objects-created-count");
    if (!countHeader) {
      throw new Error(
        "Missing x-blobd-objects-created-count header in response"
      );
    }

    return {
      successfulCount: parseInt(countHeader, 10),
    };
  }
}

// Re-export types for convenience
export type {
  AuthTokenAction,
  BatchCreateObjectEntry,
  BatchCreatedObjects,
  CreatedObject,
  InspectedObject,
  WrittenObjectPart,
};

import { encode as msgpackEncode } from "@msgpack/msgpack";
import { keyedHash } from "blake3";
import { timingSafeEqual } from "crypto";

export type AuthTokenAction =
  | { type: "BatchCreateObjects" }
  | { type: "CommitObject"; key: Uint8Array }
  | { type: "CreateObject"; key: Uint8Array; size: number }
  | { type: "DeleteObject"; key: Uint8Array }
  | { type: "InspectObject"; key: Uint8Array }
  | { type: "ReadObject"; key: Uint8Array }
  | { type: "WriteObject"; key: Uint8Array };

export class BlobdTokens {
  constructor(private readonly secret: Buffer) {
    if (secret.length !== 32) {
      throw new Error("Token secret must be exactly 32 bytes");
    }
  }

  generate<T>(tokenData: T): Uint8Array {
    const raw = msgpackEncode(tokenData);
    return new Uint8Array(keyedHash(this.secret, Buffer.from(raw)));
  }

  verify<T>(token: Uint8Array, expectedTokenData: T): boolean {
    if (token.length !== 32) {
      return false;
    }
    const expectedMac = this.generate(expectedTokenData);
    // Constant-time comparison to avoid timing attacks
    return timingSafeEqual(Buffer.from(token), Buffer.from(expectedMac));
  }
}

export class AuthToken {
  static new(
    tokens: BlobdTokens,
    action: AuthTokenAction,
    expires: number
  ): string {
    const tokenData = { action, expires };
    const tokenRaw = tokens.generate(tokenData);

    // Encode expires as big-endian u64
    const expiresBytes = new Uint8Array(8);
    const view = new DataView(expiresBytes.buffer);
    view.setBigUint64(0, BigInt(expires), false); // false = big-endian

    // Combine expires + token_raw
    const raw = new Uint8Array(8 + tokenRaw.length);
    raw.set(expiresBytes, 0);
    raw.set(tokenRaw, 8);

    return base64UrlEncode(raw);
  }

  static verify(
    tokens: BlobdTokens,
    token: string,
    expectedAction: AuthTokenAction
  ): boolean {
    const now = Math.floor(Date.now() / 1000);

    let raw: Uint8Array;
    try {
      raw = base64UrlDecode(token);
    } catch {
      return false;
    }

    if (raw.length !== 8 + 32) {
      return false;
    }

    const expiresRaw = raw.slice(0, 8);
    const tokenRaw = raw.slice(8);

    const view = new DataView(expiresRaw.buffer, expiresRaw.byteOffset);
    const expires = Number(view.getBigUint64(0, false)); // false = big-endian

    if (!tokens.verify(tokenRaw, { action: expectedAction, expires })) {
      return false;
    }

    if (expires <= now) {
      return false;
    }

    return true;
  }
}

function base64UrlEncode(data: Uint8Array): string {
  const base64 = Buffer.from(data).toString("base64");
  return base64.replace(/\+/g, "-").replace(/\//g, "_").replace(/=/g, "");
}

function base64UrlDecode(str: string): Uint8Array {
  // Add padding if needed
  const pad = (4 - (str.length % 4)) % 4;
  const base64 = str.replace(/-/g, "+").replace(/_/g, "/") + "=".repeat(pad);
  return new Uint8Array(Buffer.from(base64, "base64"));
}

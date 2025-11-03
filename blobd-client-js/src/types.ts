export interface CreatedObject {
  uploadToken: string;
}

export interface WrittenObjectPart {
  writeReceipt: string;
}

export interface InspectedObject {
  objectId: bigint;
  contentLength: number;
}

export interface BatchCreatedObjects {
  successfulCount: number;
}

export interface BatchCreateObjectEntry {
  key: string;
  size: number;
  data: ReadableStream<Uint8Array> | Uint8Array;
}

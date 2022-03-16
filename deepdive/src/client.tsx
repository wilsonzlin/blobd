import React, { useEffect, useMemo, useState } from "react";
import ReactDOM from "react-dom";

const INO_OFFSETOF_NEXT_INODE_DEV_OFFSET = 0;
const INO_OFFSETOF_STATE = INO_OFFSETOF_NEXT_INODE_DEV_OFFSET + 6;
const INO_OFFSETOF_LAST_TILE_MODE = INO_OFFSETOF_STATE + 1;
const INO_OFFSETOF_SIZE = INO_OFFSETOF_LAST_TILE_MODE + 1;
const INO_OFFSETOF_OBJ_NO = INO_OFFSETOF_SIZE + 5;
const INO_OFFSETOF_KEY_LEN = INO_OFFSETOF_OBJ_NO + 8;
const INO_OFFSETOF_KEY = INO_OFFSETOF_KEY_LEN + 1;
const INO_OFFSETOF_KEY_NULL_TERM = (keyLen: number) =>
  INO_OFFSETOF_KEY + keyLen;

const between = (v: number, low: number, high: number) => v >= low && v < high;

const readU24 = (data: Uint8Array, offset: number) => {
  let v = BigInt(data[offset]);
  v = (v << 8n) | BigInt(data[offset + 1]);
  v = (v << 8n) | BigInt(data[offset + 2]);
  return Number(v);
};

const readU40 = (data: Uint8Array, offset: number) => {
  let v = BigInt(data[offset]);
  v = (v << 8n) | BigInt(data[offset + 1]);
  v = (v << 8n) | BigInt(data[offset + 2]);
  v = (v << 8n) | BigInt(data[offset + 3]);
  v = (v << 8n) | BigInt(data[offset + 4]);
  return Number(v);
};

const readU48 = (data: Uint8Array, offset: number) => {
  let v = BigInt(data[offset]);
  v = (v << 8n) | BigInt(data[offset + 1]);
  v = (v << 8n) | BigInt(data[offset + 2]);
  v = (v << 8n) | BigInt(data[offset + 3]);
  v = (v << 8n) | BigInt(data[offset + 4]);
  v = (v << 8n) | BigInt(data[offset + 5]);
  return Number(v);
};

const byteClass = (v: number) =>
  v >= 32 && v <= 126
    ? "byte-printable"
    : v == 0
    ? "byte-null"
    : v <= 31 || v == 127
    ? "byte-control"
    : "byte-extended";

const byteText = (v: number) =>
  v % 128 >= 32 && v % 128 <= 126 ? String.fromCharCode(v % 128) : "•";

const App = ({}: {}) => {
  const [offset, setOffset] = useState(0);
  const [data, setData] = useState(() => new ArrayBuffer(0));
  useEffect(() => {
    const abortController = new AbortController();
    fetch(`/data?offset=${offset}`, {
      signal: abortController.signal,
    })
      .then((res) => res.arrayBuffer())
      .then((data) => setData(data));
    return () => abortController.abort();
  }, [offset]);
  const dataView = useMemo(() => new DataView(data), [data]);
  const dataBytes = useMemo(() => new Uint8Array(data), [data]);

  const [hoverOffset, setHoverOffset] = useState<undefined | number>(undefined);
  const hovering = hoverOffset != undefined;

  const inodeKeyLen = dataBytes[(hoverOffset ?? 0) + INO_OFFSETOF_KEY_LEN];

  const rows = Math.ceil(data.byteLength / 16);

  return (
    <div>
      <input
        type="number"
        min={0}
        step={16777216}
        value={offset}
        onChange={(e) => setOffset(e.currentTarget.valueAsNumber)}
      />
      <div id="data">
        <table>
          <thead></thead>
          <tbody>
            {Array.from({ length: rows }, (_, rowNo) => (
              <tr key={rowNo}>
                <th>{offset + rowNo * 16}</th>
                {[...dataBytes.slice(rowNo * 16, (rowNo + 1) * 16)].map(
                  (byte, i) => {
                    const idx = rowNo * 16 + i;
                    return (
                      <td
                        key={i}
                        className={[
                          idx === hoverOffset && "byte-selected",
                          byteClass(byte),
                          ...(!hovering
                            ? []
                            : [
                                between(
                                  idx - hoverOffset,
                                  INO_OFFSETOF_NEXT_INODE_DEV_OFFSET,
                                  INO_OFFSETOF_LAST_TILE_MODE
                                ) && "ino-next-inode-dev-offset",
                                between(
                                  idx - hoverOffset,
                                  INO_OFFSETOF_LAST_TILE_MODE,
                                  INO_OFFSETOF_SIZE
                                ) && "ino-last-tile-mode",
                                between(
                                  idx - hoverOffset,
                                  INO_OFFSETOF_SIZE,
                                  INO_OFFSETOF_OBJ_NO
                                ) && "ino-size",
                                between(
                                  idx - hoverOffset,
                                  INO_OFFSETOF_OBJ_NO,
                                  INO_OFFSETOF_STATE
                                ) && "ino-obj-no",
                                between(
                                  idx - hoverOffset,
                                  INO_OFFSETOF_STATE,
                                  INO_OFFSETOF_KEY_LEN
                                ) && "ino-state",
                                between(
                                  idx - hoverOffset,
                                  INO_OFFSETOF_KEY_LEN,
                                  INO_OFFSETOF_KEY
                                ) && "ino-key-len",
                                between(
                                  idx - hoverOffset,
                                  INO_OFFSETOF_KEY,
                                  INO_OFFSETOF_KEY_NULL_TERM(inodeKeyLen)
                                ) && "ino-key",
                                between(
                                  idx - hoverOffset,
                                  INO_OFFSETOF_KEY_NULL_TERM(inodeKeyLen),
                                  INO_OFFSETOF_KEY_NULL_TERM(inodeKeyLen) + 1
                                ) && "ino-key-null-terminator",
                              ]),
                        ]
                          .filter((c) => c)
                          .join(" ")}
                        onClick={() => setHoverOffset(idx)}
                      >
                        {byte}
                      </td>
                    );
                  }
                )}
                <td>
                  {[...dataBytes.slice(rowNo * 16, rowNo * 16 + 8)].map(
                    (byte, i) => (
                      <span key={i} className={byteClass(byte)}>
                        {byteText(byte)}
                      </span>
                    )
                  )}
                </td>
                <td>
                  {[...dataBytes.slice(rowNo * 16 + 8, (rowNo + 1) * 16)].map(
                    (byte, i) => (
                      <span key={i} className={byteClass(byte)}>
                        {byteText(byte)}
                      </span>
                    )
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>

        {hoverOffset == undefined ? null : (
          <dl>
            <dt>u16</dt>
            <dd>{dataView.getUint16(hoverOffset, false)}</dd>

            <dt>u24</dt>
            <dd>{readU24(dataBytes, hoverOffset)}</dd>

            <dt>u32</dt>
            <dd>{dataView.getUint32(hoverOffset, false)}</dd>

            <dt>u40</dt>
            <dd>{readU40(dataBytes, hoverOffset)}</dd>

            <dt>u48</dt>
            <dd>{readU48(dataBytes, hoverOffset)}</dd>

            <dt>u64</dt>
            <dd>{dataView.getBigUint64(hoverOffset, false).toString()}</dd>

            <dt className="ino-next-inode-dev-offset">
              Inode next inode dev offset
            </dt>
            <dd>
              {readU24(
                dataBytes,
                hoverOffset + INO_OFFSETOF_NEXT_INODE_DEV_OFFSET
              )}
            </dd>

            <dt className="ino-obj-no">Inode object number</dt>
            <dd>
              {dataView
                .getBigUint64(hoverOffset + INO_OFFSETOF_OBJ_NO, false)
                .toString()}
            </dd>

            <dt className="ino-state">Inode state</dt>
            <dd>{dataBytes[hoverOffset + INO_OFFSETOF_STATE]}</dd>

            <dt className="ino-size">Inode size</dt>
            <dd>{readU40(dataBytes, hoverOffset + INO_OFFSETOF_SIZE)}</dd>

            <dt className="ino-last-tile-mode">Inode last tile mode</dt>
            <dd>{dataBytes[hoverOffset + INO_OFFSETOF_LAST_TILE_MODE]}</dd>

            <dt className="ino-key-len">Inode key length</dt>
            <dd>{inodeKeyLen}</dd>

            <dt className="ino-key">Inode key</dt>
            <dd>
              {[
                ...dataBytes.slice(
                  hoverOffset + INO_OFFSETOF_KEY,
                  hoverOffset + INO_OFFSETOF_KEY + inodeKeyLen
                ),
              ]
                .map((c) => String.fromCharCode(c))
                .join("")}
            </dd>

            <dt className="ino-key-null-terminator">
              Inode key null terminator
            </dt>
            <dd>
              {dataBytes[hoverOffset + INO_OFFSETOF_KEY_NULL_TERM(inodeKeyLen)]}
            </dd>
          </dl>
        )}
      </div>
    </div>
  );
};

ReactDOM.render(<App />, document.querySelector("#root"));

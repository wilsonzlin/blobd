import React, { useEffect, useMemo, useState } from "react";
import ReactDOM from "react-dom";

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

const Byte = ({
  decimal,
  children,
}: {
  decimal?: boolean;
  children: number;
}) => {
  const text = decimal
    ? children
    : children % 128 >= 32 && children % 128 <= 126
    ? String.fromCharCode(children % 128)
    : "•";
  const color =
    children >= 32 && children <= 126
      ? "aqua"
      : children == 0
      ? "gray"
      : children <= 31 || children == 127
      ? "pink"
      : "gold";
  return <span style={{ color }}>{text}</span>;
};

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

  const inodeKeyLen = dataBytes[hoverOffset ?? 0 + 24];

  const rows = Math.ceil(data.byteLength / 16);

  return (
    <div>
      <input
        type="number"
        min={1}
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
                  (byte, i) => (
                    <td
                      key={i}
                      onMouseOver={() => setHoverOffset(rowNo * 16 + i)}
                      onMouseOut={() => setHoverOffset(undefined)}
                    >
                      <Byte decimal>{byte}</Byte>
                    </td>
                  )
                )}
                <td>
                  {[...dataBytes.slice(rowNo * 16, rowNo * 16 + 8)].map(
                    (byte, i) => (
                      <Byte key={i}>{byte}</Byte>
                    )
                  )}
                </td>
                <td>
                  {[...dataBytes.slice(rowNo * 16 + 8, (rowNo + 1) * 16)].map(
                    (byte, i) => (
                      <Byte key={i}>{byte}</Byte>
                    )
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>

        {!hoverOffset ? null : (
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

            <dt>Inode inode size</dt>
            <dd>{readU24(dataBytes, hoverOffset + 0)}</dd>

            <dt>Inode next inode tile</dt>
            <dd>{readU24(dataBytes, hoverOffset + 3)}</dd>

            <dt>Inode next inode tile offset</dt>
            <dd>{readU24(dataBytes, hoverOffset + 6)}</dd>

            <dt>Inode object number</dt>
            <dd>{dataView.getBigUint64(hoverOffset + 9, false).toString()}</dd>

            <dt>Inode state</dt>
            <dd>{dataBytes[hoverOffset + 17]}</dd>

            <dt>Inode size</dt>
            <dd>{readU40(dataBytes, hoverOffset + 18)}</dd>

            <dt>Inode last tile mode</dt>
            <dd>{dataBytes[hoverOffset + 23]}</dd>

            <dt>Inode key length</dt>
            <dd>{inodeKeyLen}</dd>

            <dt>Inode key</dt>
            <dd>
              {[
                ...dataBytes.slice(
                  hoverOffset + 25,
                  hoverOffset + 25 + inodeKeyLen
                ),
              ]
                .map((c) => String.fromCharCode(c))
                .join("")}
            </dd>

            <dt>Inode key null terminator</dt>
            <dd>{dataBytes[hoverOffset + 25 + inodeKeyLen]}</dd>
          </dl>
        )}
      </div>
    </div>
  );
};

ReactDOM.render(<App />, document.querySelector("#root"));

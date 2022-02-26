import Plot from "react-plotly.js";
import { AxisType } from "plotly.js";
import React from "react";
import ReactDOM from "react-dom";

const formatBytes = (bytes: number) => {
  let p: string | undefined;
  for (const prefix of ["", "K", "M", "G", "T"]) {
    if (bytes < 950) {
      p = prefix;
      break;
    }
    bytes /= 1024;
  }
  p ??= "P";
  let val = bytes.toFixed(2);
  if (val.includes(".")) {
    val = val.replace(/0+$/, "").replace(/\.$/, "");
  }
  return `${val} ${p}B`;
};

const chartLayout = ({
  xType,
  yType,
  xTitle,
  yTitle,
  xTickText,
  xTickVals,
  yTickText,
  yTickVals,
  yTickFormat,
  marginLeft,
}: {
  xType?: AxisType;
  yType?: AxisType;
  xTitle?: string;
  yTitle?: string;
  xTickVals?: number[];
  xTickText?: string[];
  yTickVals?: number[];
  yTickText?: string[];
  yTickFormat?: string;
  marginLeft?: number;
}) =>
  ({
    width: 1600,
    height: 480,
    xaxis: {
      type: xType,
      title: xTitle,
      tickvals: xTickVals,
      ticktext: xTickText,
    },
    yaxis: {
      type: yType,
      title: yTitle,
      tickvals: yTickVals,
      ticktext: yTickText,
      tickformat: yTickFormat,
    },
    margin: { t: 0, l: marginLeft },
  } as const);

const tilesChartLayout = (props: {
  yTitle?: string;
  yTickVals?: number[];
  yTickText?: string[];
  yTickFormat?: string;
  marginLeft?: number;
}) =>
  chartLayout({
    ...props,
    xType: "log",
    yType: "log",
    xTitle: "Tile size",
    xTickText: TILE_SIZES_FMT,
    xTickVals: TILE_SIZES,
  });

const TILE_PTR_WIDTH = 32;
const OBJ_SIZE_IN_TILE_UNITS_WIDTH = 16;

const TILE_SIZES = [
  8, 16, 64, 128, 1024, 2048, 4096, 8192, 16384, 65536, 131072, 524288, 1048576,
  4194304, 8388608, 16777216,
];
const TILE_SIZES_FMT = TILE_SIZES.map(formatBytes);

const OBJ_SIZES = [
  1024 * 50,
  1024 * 120,
  1024 * 270,
  1024 * 1024 * 1.2,
  1024 * 1024 * 6,
  1024 * 1024 * 20,
  1024 * 1024 * 80,
  1024 * 1024 * 190,
  1024 * 1024 * 450,
  1024 * 1024 * 1024 * 1.3,
  1024 * 1024 * 1024 * 4.5,
  1024 * 1024 * 1024 * 13,
];

const PHYSICAL_BYTES = TILE_SIZES.map((sz) => sz * 2 ** TILE_PTR_WIDTH);

const PHYSICAL_BYTES_FMT = PHYSICAL_BYTES.map(formatBytes);

const PhysicalBytes = () => (
  <Plot
    data={[
      {
        x: TILE_SIZES,
        y: PHYSICAL_BYTES,
      },
    ]}
    layout={tilesChartLayout({
      yTitle: "Max volume size",
      yTickVals: PHYSICAL_BYTES,
      yTickText: PHYSICAL_BYTES_FMT,
    })}
  />
);

const MAX_OBJ_SIZES = TILE_SIZES.map(
  (sz) => sz * 2 ** OBJ_SIZE_IN_TILE_UNITS_WIDTH
);

const MaxObjSize = () => (
  <Plot
    data={[
      {
        x: TILE_SIZES,
        y: MAX_OBJ_SIZES,
      },
    ]}
    layout={tilesChartLayout({
      yTitle: "Max object size",
      yTickVals: MAX_OBJ_SIZES,
      yTickText: MAX_OBJ_SIZES.map(formatBytes),
    })}
  />
);

const BKT_LOAD_FACTOR_TARGET = 0.75;

const BUCKET_POINTER_STORAGE_PROPORTIONS = [0.00001, 0.0001, 0.001, 0.01, 0.02];

const MaxObjCount = () => (
  <Plot
    data={[
      ...OBJ_SIZES.map((objSize) => ({
        x: PHYSICAL_BYTES,
        y: PHYSICAL_BYTES.map((t) => Math.floor(t / objSize)),
        name: `${formatBytes(objSize)} objects`,
      })),
      ...BUCKET_POINTER_STORAGE_PROPORTIONS.map((ppt) => ({
        x: PHYSICAL_BYTES,
        y: PHYSICAL_BYTES.map(
          (t) =>
            Math.floor((t * ppt) / (TILE_PTR_WIDTH / 8)) *
            BKT_LOAD_FACTOR_TARGET
        ),
        mode: "lines",
        name: `${ppt * 100}% bucket pointer storage`,
        line: {
          dash: "dot",
        },
      })),
    ]}
    layout={chartLayout({
      xType: "log",
      yType: "log",
      xTitle: "Disk size",
      yTitle: "Max objects",
      xTickVals: PHYSICAL_BYTES,
      xTickText: PHYSICAL_BYTES_FMT,
    })}
  />
);

// We use an absolute disk size to make aware of the actual physical impact.
const DiskDataStructures = ({ diskSize }: { diskSize: number }) => {
  const OVERHEADS = [8, 16, 32, 64, 128];
  // We assume pathological worst case where every tile's address is recorded along with some other data (counted as overhead).
  // Note that space reserved for this metadata cannot be used as tiles.
  const calcYVal = (overheadBytes: number, tileSize: number) => {
    const metadataBytesPerTile = TILE_PTR_WIDTH / 8 + overheadBytes;
    const tiles = Math.ceil(diskSize / tileSize);
    return metadataBytesPerTile * tiles;
  };
  const yTickVals = TILE_SIZES.map((tileSize) =>
    calcYVal(OVERHEADS[2], tileSize)
  );
  return (
    <Plot
      data={OVERHEADS.map((overheadBytes) => ({
        x: TILE_SIZES,
        y: TILE_SIZES.map((tileSize) => calcYVal(overheadBytes, tileSize)),
        name: `${formatBytes(overheadBytes)} overhead`,
      }))}
      layout={tilesChartLayout({
        yTitle: "Storage used",
        marginLeft: 150,
        yTickVals,
        yTickText: yTickVals.map(
          (usage) =>
            `${formatBytes(usage)} (${
              Math.round((usage / diskSize) * 10000) / 100
            }%)`
        ),
      })}
    />
  );
};

// We use an absolute disk size to make aware of the actual physical impact.
const BucketPointers = ({ diskSize }: { diskSize: number }) => {
  const BUCKETS = Array(8)
    .fill(0)
    .map((_, i) => 2 ** (24 + i));
  const BUCKET_POINTER_CHECKSUM_INTERVAL = [1, 2, 4, 8, 16, 32, 64, 128, 256];
  const calcYVal = (buckets: number, bucketPointerChecksumInterval: number) => {
    return buckets * 6 + Math.ceil(buckets / bucketPointerChecksumInterval) * 8;
  };
  const yTickVals = BUCKETS.map((bkts) =>
    calcYVal(bkts, BUCKET_POINTER_CHECKSUM_INTERVAL[4])
  );
  return (
    <Plot
      data={BUCKET_POINTER_CHECKSUM_INTERVAL.map((bpci) => ({
        x: BUCKETS,
        y: BUCKETS.map((bkts) => calcYVal(bkts, bpci)),
        name: `Checksum every ${bpci} bucket pointer${bpci == 1 ? "" : "s"}`,
      }))}
      layout={chartLayout({
        xType: "log",
        yType: "log",
        xTitle: "Buckets",
        yTitle: "Storage used",
        marginLeft: 150,
        yTickVals,
        yTickText: yTickVals.map(
          (usage) =>
            `${formatBytes(usage)} (${
              Math.round((usage / diskSize) * 10000) / 100
            }%)`
        ),
      })}
    />
  );
};

const MICROTILE_METADATA_OVERHEAD = 8;

// We use an absolute disk size to make aware of the actual physical impact.
const Microtiles = ({ diskSize }: { diskSize: number }) => {
  const MICROTILE_PROPORTIONS = [0.1, 0.2, 0.3, 0.4, 0.5, 0.75, 0.9];
  const calcYVal = (microtileProportion: number, tileSize: number) => {
    const allTiles = Math.ceil(diskSize / tileSize);
    const microtiles = Math.round(allTiles * microtileProportion);
    const microtileAddrBytes = Math.max(1, Math.ceil(Math.log2(tileSize) / 8));
    return microtiles * (microtileAddrBytes + MICROTILE_METADATA_OVERHEAD);
  };
  const yTickVals = TILE_SIZES.map((tileSize) =>
    calcYVal(MICROTILE_PROPORTIONS[3], tileSize)
  );
  return (
    <Plot
      data={MICROTILE_PROPORTIONS.map((microtileProportion) => ({
        x: TILE_SIZES,
        y: TILE_SIZES.map((tileSize) =>
          calcYVal(microtileProportion, tileSize)
        ),
        name: `${microtileProportion * 100}% of tiles are microtiles`,
      }))}
      layout={tilesChartLayout({
        yTitle: "Storage required to track microtiles",
        marginLeft: 150,
        yTickVals,
        yTickText: yTickVals.map(
          (usage) =>
            `${formatBytes(usage)} (${
              Math.round((usage / diskSize) * 10000) / 100
            })%`
        ),
      })}
    />
  );
};

// We use an absolute disk size to make aware of the actual physical impact.
const Inodes = ({ diskSize }: { diskSize: number }) => {
  const OBJECTS = [1e6, 1e7, 1e8, 1e9, 1e10];
  const INODE_SIZES = [128, 256, 1024, 4096, 16384, 65536];
  const calcYVal = (objects: number, inodeSize: number) => {
    return objects * inodeSize;
  };
  const yTickVals = OBJECTS.map((objects) => calcYVal(objects, INODE_SIZES[5]));
  return (
    <Plot
      data={INODE_SIZES.map((inodeSize) => ({
        x: OBJECTS,
        y: OBJECTS.map((objects) => calcYVal(objects, inodeSize)),
        name: formatBytes(inodeSize),
      }))}
      layout={chartLayout({
        xType: "log",
        yType: "log",
        xTitle: "Objects",
        yTitle: "Storage used",
        marginLeft: 150,
        yTickVals,
        yTickText: yTickVals.map(
          (usage) =>
            `${formatBytes(usage)} (${
              Math.round((usage / diskSize) * 10000) / 100
            }%)`
        ),
      })}
    />
  );
};

// We use an absolute disk size to make aware of the actual physical impact.
const TileWaste = ({ diskSize }: { diskSize: number }) => {
  const calcYVal = (tileSize: number, objectSize: number) => {
    const totalTiles = Math.ceil(diskSize / tileSize);
    const totalObjects = totalTiles / Math.ceil(objectSize / tileSize);
    return (tileSize / 2) * totalObjects;
  };
  const yTickVals = TILE_SIZES.map((tileSize) =>
    calcYVal(tileSize, OBJ_SIZES[7])
  );
  return (
    <Plot
      data={OBJ_SIZES.map((objSize) => ({
        x: TILE_SIZES,
        y: TILE_SIZES.map((tileSize) => calcYVal(tileSize, objSize)),
        name: formatBytes(objSize),
      }))}
      layout={chartLayout({
        xType: "log",
        yType: "log",
        xTitle: "Tile sizes",
        xTickVals: TILE_SIZES,
        xTickText: TILE_SIZES_FMT,
        yTitle: "Waste",
        marginLeft: 150,
        yTickVals,
        yTickText: yTickVals.map(
          (usage) =>
            `${formatBytes(usage)} (${
              Math.round((usage / diskSize) * 10000) / 100
            }%)`
        ),
      })}
    />
  );
};

// We use an absolute disk size to make aware of the actual physical impact.
const WorstCaseExtents = ({ diskSize }: { diskSize: number }) => {
  const calcYVal = (tileSize: number, objectSize: number) => {
    const totalTiles = Math.ceil(diskSize / tileSize);
    const tilesPerObject = Math.ceil(objectSize / tileSize);
    const totalObjects = totalTiles / tilesPerObject;
    // Assume 4 bytes for offset within object, 4 bytes for tile address, and 8 bytes for checksum and other state.
    return 16 * tilesPerObject * totalObjects;
  };
  const yTickVals = TILE_SIZES.map((tileSize) =>
    calcYVal(tileSize, OBJ_SIZES[7])
  );
  return (
    <Plot
      data={OBJ_SIZES.map((objSize) => ({
        x: TILE_SIZES,
        y: TILE_SIZES.map((tileSize) => calcYVal(tileSize, objSize)),
        name: formatBytes(objSize),
      }))}
      layout={chartLayout({
        xType: "log",
        yType: "log",
        xTitle: "Tile sizes",
        xTickVals: TILE_SIZES,
        xTickText: TILE_SIZES_FMT,
        yTitle: "Space needed for extents of all objects",
        marginLeft: 150,
        yTickVals,
        yTickText: yTickVals.map(
          (usage) =>
            `${formatBytes(usage)} (${
              Math.round((usage / diskSize) * 10000) / 100
            }%)`
        ),
      })}
    />
  );
};

const App = () => (
  <div className="App">
    <section>
      <h1>Maximum volume sizes</h1>
      <PhysicalBytes />
    </section>
    <section>
      <h1>Maximum object sizes</h1>
      <MaxObjSize />
    </section>
    <section>
      <h1>Maximum object count</h1>
      <MaxObjCount />
    </section>
    <section>
      <h1>Disk data structure overhead, 16 TiB disk</h1>
      <DiskDataStructures diskSize={16 * 1024 * 1024 * 1024 * 1024} />
    </section>
    <section>
      <h1>Bucket pointers, 16 TiB disk</h1>
      <BucketPointers diskSize={16 * 1024 * 1024 * 1024 * 1024} />
    </section>
    <section>
      <h1>Microtile data structure overhead, 16 TiB disk</h1>
      <Microtiles diskSize={16 * 1024 * 1024 * 1024 * 1024} />
    </section>
    <section>
      <h1>Inodes, 16 TiB disk</h1>
      <Inodes diskSize={16 * 1024 * 1024 * 1024 * 1024} />
    </section>
    <section>
      <h1>Tile waste, 16 TiB disk</h1>
      <TileWaste diskSize={16 * 1024 * 1024 * 1024 * 1024} />
    </section>
    <section>
      <h1>Object extents, 16 TiB disk</h1>
      <WorstCaseExtents diskSize={16 * 1024 * 1024 * 1024 * 1024} />
    </section>
  </div>
);

ReactDOM.render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
  document.getElementById("root")
);

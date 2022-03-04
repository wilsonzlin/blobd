import Plot from "react-plotly.js";
import { AxisType } from "plotly.js";
import React, { useMemo, useState } from "react";
import ReactDOM from "react-dom";

const distinctYTickVals = (yTickVals: number[]) => {
  const distinct = [];
  for (const v of yTickVals.sort((a, b) => a - b)) {
    if (!distinct.length || v >= distinct[distinct.length - 1] * 2.5) {
      distinct.push(v);
    }
  }
  return distinct;
};

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

const TILE_PTR_WIDTH = 24;
const OBJ_SIZE_IN_TILE_UNITS_WIDTH = 16;

const TILE_SIZES = [
  8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576, 2097152,
  4194304, 8388608, 16777216, 33554432, 67108864
];
const TILE_SIZES_FMT = TILE_SIZES.map(formatBytes);

const OBJ_SIZES = [
  52497,
  120094,
  270662,
  1273409,
  6984770,
  21338554,
  80001001,
  190991576,
  455130717,
  1537460911,
  4900140055,
  14383222003,
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
  const yTickVals = TILE_SIZES.map((microtileSize) => Math.ceil(diskSize / microtileSize) / 8);
  return (
    <Plot
      data={[{
        x: TILE_SIZES,
        y: yTickVals,
      }]}
      layout={chartLayout({
        xType: "log",
        yType: "log",
        xTitle: "Microtile size",
        xTickText: TILE_SIZES_FMT,
        xTickVals: TILE_SIZES,
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
  const BUCKETS = Array(17)
    .fill(0)
    .map((_, i) => 2 ** (24 + i));
  const yTickVals = BUCKETS.map((bkts) => bkts * (TILE_PTR_WIDTH / 8 + 1));
  return (
    <Plot
      data={[{
        x: BUCKETS,
        y: yTickVals,
      }]}
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

// We use an absolute disk size to make aware of the actual physical impact.
const Inodes = ({ diskSize }: { diskSize: number }) => {
  const OBJECTS = [1e6, 1e7, 1e8, 1e9, 1e10];
  const INODE_SIZES = [128, 256, 1024, 4096, 16384, 65536];
  const calcYVal = (objects: number, inodeSize: number) => {
    return objects * inodeSize;
  };
  const yTickVals = [
    ...OBJECTS.map((objects) => calcYVal(objects, INODE_SIZES[1])),
    ...OBJECTS.map((objects) => calcYVal(objects, INODE_SIZES[5])),
  ];
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
    const tilesUsedPerObject = Math.ceil(objectSize / tileSize);
    const totalTiles = Math.floor(diskSize / tileSize);
    const totalObjects = Math.floor(totalTiles / tilesUsedPerObject);
    return (tileSize - (objectSize % tileSize)) * totalObjects;
  };
  const yTickVals = distinctYTickVals(OBJ_SIZES.flatMap(objSize => TILE_SIZES.map((tileSize) =>
    calcYVal(tileSize, objSize)
  )));
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
const BlockChecksums = ({ diskSize }: { diskSize: number }) => {
  const BLOCK_SIZES = [
    512,
    1024,
    4 * 1024,
    16 * 1024,
    64 * 1024,
    256 * 1024,
    512 * 1024,
    1024 * 1024,
    4 * 1024 * 1024,
    16 * 1024 * 1024,
  ];
  const calcYVal = (blockSize: number) => {
    return Math.ceil(diskSize / blockSize) * 8;
  };
  const yTickVals = BLOCK_SIZES.map((blockSize) => calcYVal(blockSize));
  return (
    <Plot
      data={[
        {
          x: BLOCK_SIZES,
          y: yTickVals,
        },
      ]}
      layout={chartLayout({
        xType: "log",
        yType: "log",
        xTitle: "Block sizes",
        xTickVals: BLOCK_SIZES,
        xTickText: BLOCK_SIZES.map(formatBytes),
        yTitle: "Checksum storage",
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

const App = () => {
  const [diskSize, setDiskSize] = useState(64 * 1024 * 1024 * 1024 * 1024);
  const diskSizeFmt = useMemo(() => formatBytes(diskSize), [diskSize]);

  return (
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
        <h1>Freelist, {diskSizeFmt} disk</h1>
        <p>Regardless of how we arrange the split between tile and microtile, we need one bit per microtile. The entire freelist must reside in memory.</p>
        <DiskDataStructures diskSize={diskSize} />
      </section>
      <section>
        <h1>Bucket pointers, {diskSizeFmt} disk</h1>
        <BucketPointers diskSize={diskSize} />
      </section>
      <section>
        <h1>Inodes, {diskSizeFmt} disk</h1>
        <Inodes diskSize={diskSize} />
      </section>
      <section>
        <h1>Tile waste, {diskSizeFmt} disk</h1>
        <TileWaste diskSize={diskSize} />
      </section>
      <section>
        <h1>Block checksums, {diskSizeFmt} disk</h1>
        <BlockChecksums diskSize={diskSize} />
      </section>
      <section>
        <h1>Object extents, {diskSizeFmt} disk</h1>
        <WorstCaseExtents diskSize={diskSize} />
      </section>
    </div>
  );
};

ReactDOM.render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
  document.getElementById("root")
);

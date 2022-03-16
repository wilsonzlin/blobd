import express from "express";
import fs from "fs";
import stream from "stream/promises";

const DEV = process.argv[2];

const server = express();

server.set("query parser", "simple");

server.get("/", (_req, res) => {
  res.sendFile(`${__dirname}/client.html`);
});

server.get("/data", (req, res) => {
  const start = Number.parseInt(req.query["offset"] as any, 10);
  const end = start + 1024;
  console.log("Reading", start, "to", end);
  stream.pipeline(
    fs.createReadStream(DEV, {
      start,
      end,
    }),
    res.status(200)
  );
  return;
});

server.listen(8181, () => console.log("Listening"));

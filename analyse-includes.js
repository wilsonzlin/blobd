/**

Use this script to generate a Mermaid graph that visualises includes. Cyclic imports can be easily found by finding the occasional arrow in the opposite direction.

**/

const fs = require("fs");
const path = require("path");

const BASE = `${__dirname}/src`;

console.log("graph TD");
const visitDir = (relPath) => {
  const dirents = fs.readdirSync(`${BASE}/${relPath}`, {withFileTypes: true});
  for (const de of dirents) {
    if (de.isDirectory()) {
      visitDir(`${relPath}/${de.name}`);
    } else if (de.isFile() && de.name.endsWith(".h")) {
      const raw = fs.readFileSync(`${BASE}/${relPath}/${de.name}`, "utf8");
      for (const [_, imp] of raw.matchAll(/^#include "(.*?)"$/gm)) {
        const dest = path.relative(BASE, `${BASE}/${relPath}/${imp}`);
        console.log(`${relPath}/${de.name}-->${dest}`.slice(1));
      }
    }
  }
};

visitDir("");

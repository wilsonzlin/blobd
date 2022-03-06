const esbuild = require("esbuild");
const fs = require("fs");

esbuild.buildSync({
  bundle: true,
  entryPoints: ["src/server.ts"],
  outfile: "dist/server.js",
  platform: "node",
  write: true,
});

const clientJs = esbuild.buildSync({
  bundle: true,
  entryPoints: ["src/client.tsx"],
  legalComments: "none",
  write: false,
});

fs.writeFileSync(
  `${__dirname}/dist/client.html`,
  fs
    .readFileSync(`${__dirname}/src/client.html`, "utf-8")
    .replace("</body>", () =>
      [
        `<style>${fs.readFileSync(
          `${__dirname}/src/client.css`,
          "utf-8"
        )}</style>`,
        `<script>${clientJs.outputFiles[0].text}</script>`,
        `</body>`,
      ].join("")
    )
);

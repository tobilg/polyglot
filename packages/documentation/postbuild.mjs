import { readdir, readFile, writeFile, cp } from "node:fs/promises";
import { join } from "node:path";

const DIST = "dist";
const OG_TAGS = `<meta property="og:type" content="website" />
<meta property="og:title" content="Polyglot SQL API Documentation" />
<meta property="og:description" content="Transpile SQL between more than 30 SQL dialects in the browser" />
<meta property="og:image" content="https://polyglot.gh.tobilg.com/polyglot-opengraph.png" />
<meta name="twitter:card" content="summary_large_image" />
<meta name="twitter:title" content="Polyglot SQL API Documentation" />
<meta name="twitter:description" content="Transpile SQL between more than 30 SQL dialects in the browser" />
<meta name="twitter:image" content="https://polyglot.gh.tobilg.com/polyglot-opengraph.png" />`;

async function findHtmlFiles(dir) {
  const files = [];
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) files.push(...(await findHtmlFiles(path)));
    else if (entry.name.endsWith(".html")) files.push(path);
  }
  return files;
}

const htmlFiles = await findHtmlFiles(DIST);
for (const file of htmlFiles) {
  const content = await readFile(file, "utf8");
  if (!content.includes("og:title")) {
    await writeFile(file, content.replace("</head>", `${OG_TAGS}\n</head>`));
  }
}

await cp("public", DIST, { recursive: true });

console.log(`Injected OG tags into ${htmlFiles.length} HTML files and copied public assets.`);

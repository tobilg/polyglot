import { execSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

function readJson(filePath) {
  return JSON.parse(readFileSync(filePath, "utf8"));
}

function run(command, cwd) {
  execSync(command, { cwd, stdio: "inherit" });
}

async function readDistVersion(distIndexPath) {
  if (!existsSync(distIndexPath)) return null;
  try {
    const mod = await import(`${pathToFileURL(distIndexPath).href}?t=${Date.now()}`);
    if (typeof mod.getVersion === "function") {
      return String(mod.getVersion());
    }
  } catch {
    return null;
  }
  return null;
}

const scriptDir = dirname(fileURLToPath(import.meta.url));
const rootDir = resolve(scriptDir, "../../..");
const sdkDir = resolve(rootDir, "packages/sdk");

const sdkPackageJsonPath = resolve(sdkDir, "package.json");
const sdkWasmPackageJsonPath = resolve(sdkDir, "wasm/package.json");
const sdkDistIndexPath = resolve(sdkDir, "dist/index.js");

const expectedVersion = readJson(sdkPackageJsonPath).version;
const currentWasmVersion = existsSync(sdkWasmPackageJsonPath)
  ? readJson(sdkWasmPackageJsonPath).version
  : null;
const currentDistVersion = await readDistVersion(sdkDistIndexPath);

const needsBuild =
  currentWasmVersion !== expectedVersion || currentDistVersion !== expectedVersion;

if (needsBuild) {
  console.log(
    `[prepare:sdk] Rebuilding SDK artifacts (expected=${expectedVersion}, wasm=${currentWasmVersion ?? "missing"}, dist=${currentDistVersion ?? "missing"})`,
  );
  run("pnpm run build:wasm", sdkDir);
  run("pnpm run build", sdkDir);
} else {
  console.log(`[prepare:sdk] SDK artifacts already at version ${expectedVersion}`);
}

const finalWasmVersion = existsSync(sdkWasmPackageJsonPath)
  ? readJson(sdkWasmPackageJsonPath).version
  : null;
const finalDistVersion = await readDistVersion(sdkDistIndexPath);

if (finalWasmVersion !== expectedVersion || finalDistVersion !== expectedVersion) {
  throw new Error(
    `[prepare:sdk] Version mismatch after prepare step: expected=${expectedVersion}, wasm=${finalWasmVersion ?? "missing"}, dist=${finalDistVersion ?? "missing"}`,
  );
}

console.log(`[prepare:sdk] SDK artifacts ready at version ${expectedVersion}`);

# Polyglot SQL Playground

The playground runs SQL operations in a Web Worker using `@polyglot-sql/sdk/manual`.

## Production

`pnpm run build` bundles the SDK JavaScript loader. The application downloads
WASM at runtime from jsDelivr, falling back to UNPKG if initialization fails. Both URLs pin the
exact version from `packages/sdk/package.json`, and startup checks the loaded
WASM version before processing SQL requests.

The WASM file comes from the existing npm release:

```text
https://cdn.jsdelivr.net/npm/@polyglot-sql/sdk@<version>/dist/polyglot_sql.wasm
https://unpkg.com/@polyglot-sql/sdk@<version>/dist/polyglot_sql.wasm
```

Production builds contain no bundled `.wasm` asset. Publish the matching SDK to
npm before deploying the playground; the existing release workflow already
enforces this order. There is no separate asset upload or CDN publishing step.
If both CDNs fail, the playground displays a startup error.

The bundled JavaScript bindings must come from the same SDK build as the
published WASM. The release workflow already shares those build artifacts
between npm publishing and playground deployment. For local production builds,
use the matching published SDK's `dist` artifacts; rebuilding WASM locally with
a different toolchain can produce incompatible bindings even at the same
package version. Use development mode to test local SDK builds.

## Local development

`pnpm run dev` loads `packages/sdk/dist/polyglot_sql.wasm` through Vite's local
file server, allowing unpublished SDK changes to be tested without using a CDN.
Build the SDK before starting the playground:

```sh
pnpm --dir packages/sdk run build:wasm
pnpm --dir packages/sdk run build
pnpm --dir packages/playground run dev
```

Run these commands from the repository root. The SDK JavaScript and local WASM
must have matching versions.

# @polyglot-sql/python-docs

Cloudflare Pages site for the Python bindings API documentation.

## Build

```bash
pnpm run build
```

This command generates docs from `crates/polyglot-sql-python/docs` using `mkdocs` and writes the output to `packages/python-docs/dist` (overwrite mode via `--clean`).

## Deploy

```bash
pnpm run deploy
```

# Polyglot SQL Playground (Next.js)

A Next.js + Turbopack playground that mirrors GrowthBook's structure. Demonstrates `@polyglot-sql/sdk` via `playground-shared` (top-level import, like GrowthBook's shared package).

## Setup

From the polyglot repo root:

```bash
pnpm install
pnpm --filter @polyglot-sql/playground-next run dev
```

Open http://localhost:3000.

## Structure (mirrors GrowthBook)

- **FormatDemo** – Loaded with `ssr: false`; imports from `playground-shared/sql` (top-level SDK import like GrowthBook)
- **playground-shared** – Wraps `@polyglot-sql/sdk`; `formatWithPolyglot` throws if format is undefined (top-level await issue)

## Configuration

- Next.js 16 with Turbopack
- Pages Router
- `transpilePackages: ["@polyglot-sql/sdk", "playground-shared"]`

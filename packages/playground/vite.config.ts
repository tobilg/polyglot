import { defineConfig, normalizePath } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import wasm from "vite-plugin-wasm";
import path from "path";

export default defineConfig({
  define: {
    __POLYGLOT_LOCAL_WASM_URL__: JSON.stringify(
      `/@fs/${normalizePath(path.resolve(__dirname, "../sdk/dist/polyglot_sql.wasm"))}`,
    ),
  },
  plugins: [react(), tailwindcss(), wasm()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  assetsInclude: ["**/*.wasm"],
  optimizeDeps: {
    exclude: ["@polyglot-sql/sdk", "@polyglot-sql/sdk/manual"],
  },
  worker: { format: "es", plugins: () => [wasm()] },
  build: {
    target: "esnext",
  },
});

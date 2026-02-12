import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { init } from "@polyglot-sql/sdk";
import "./index.css";
import App from "./App";

async function bootstrap() {
  await init();
  const root = document.getElementById("root")!;
  createRoot(root).render(
    <StrictMode>
      <App />
    </StrictMode>
  );
}

bootstrap();

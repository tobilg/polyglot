import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { init } from "@polyglot-sql/sdk";
import "./index.css";
import App from "./App";

async function bootstrap() {
  await init();
  const root = document.getElementById("root")!;
  createRoot(root).render(
    <StrictMode>
      <BrowserRouter>
        <App />
      </BrowserRouter>
    </StrictMode>
  );
}

bootstrap();

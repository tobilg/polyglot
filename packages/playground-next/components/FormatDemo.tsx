import {
  initPolyglotFormat,
  formatWithPolyglot,
} from "playground-shared/sql";
import { useState, useEffect, useCallback } from "react";

const DEFAULT_SQL = `select u.id,u.name,u.email,count(o.id) as order_count from users u left join orders o on u.id=o.user_id where u.active=true group by u.id,u.name,u.email limit 50;`;

export default function FormatDemo() {
  const [sql, setSql] = useState(DEFAULT_SQL);
  const [output, setOutput] = useState("");
  const [initDone, setInitDone] = useState(false);

  useEffect(() => {
    initPolyglotFormat().then(() => setInitDone(true));
  }, []);

  const handleFormat = useCallback(() => {
    setOutput("");
    try {
      const result = formatWithPolyglot(sql, "postgresql");
      setOutput(result ?? "");
    } catch (e) {
      setOutput(String(e));
    }
  }, [sql]);

  return (
    <div
      style={{
        maxWidth: 800,
        margin: "0 auto",
        padding: 24,
        fontFamily: "system-ui",
      }}
    >
      <h1 style={{ marginBottom: 24 }}>Polyglot SQL Playground</h1>

      <div style={{ marginBottom: 16 }}>
        <textarea
          value={sql}
          onChange={(e) => setSql(e.target.value)}
          style={{
            width: "100%",
            minHeight: 120,
            padding: 12,
            fontFamily: "ui-monospace, monospace",
            fontSize: 13,
            color: "#18181b",
            background: "#fff",
            border: "1px solid #e4e4e7",
            borderRadius: 6,
            resize: "vertical",
          }}
          spellCheck={false}
        />
      </div>

      <button
        onClick={handleFormat}
        disabled={!initDone}
        style={{
          padding: "8px 16px",
          background: initDone ? "#18181b" : "#d4d4d8",
          color: "white",
          border: "none",
          borderRadius: 6,
          cursor: initDone ? "pointer" : "not-allowed",
          fontWeight: 500,
          marginBottom: 16,
        }}
      >
        Format
      </button>

      <div
        style={{
          fontSize: 12,
          color: "#71717a",
          marginBottom: 8,
        }}
      >
        Output
      </div>
      <div
        style={{
          width: "100%",
          minHeight: 120,
          padding: 12,
          fontFamily: "ui-monospace, monospace",
          fontSize: 13,
          border: "1px solid #e4e4e7",
          borderRadius: 6,
          background: "#fafafa",
          color: "#18181b",
          whiteSpace: "pre-wrap",
          wordBreak: "break-all",
        }}
      >
        {output}
      </div>
    </div>
  );
}

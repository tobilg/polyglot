import { EditorView } from "@codemirror/view";
import { Extension } from "@codemirror/state";
import { useMemo, useEffect, useState } from "react";

/* ------------------------------------------------------------------ */
/*  Light theme                                                        */
/* ------------------------------------------------------------------ */
const lightTheme = EditorView.theme(
  {
    "&": {
      backgroundColor: "var(--code-bg)",
      color: "var(--code-text)",
      fontSize: "0.875rem",
      lineHeight: "1.625",
    },
    ".cm-content": {
      caretColor: "var(--code-caret)",
      fontFamily: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace",
      padding: "0.75rem 0",
    },
    ".cm-gutters": {
      backgroundColor: "var(--code-bg)",
      borderRight: "none",
      color: "var(--code-placeholder)",
    },
    ".cm-activeLineGutter": {
      backgroundColor: "transparent",
    },
    ".cm-activeLine": {
      backgroundColor: "transparent",
    },
    ".cm-cursor": {
      borderLeftColor: "var(--code-caret)",
    },
    ".cm-selectionBackground": {
      backgroundColor: "rgba(0,0,0,0.08) !important",
    },
    "&.cm-focused .cm-selectionBackground": {
      backgroundColor: "rgba(0,0,0,0.12) !important",
    },
    ".cm-placeholder": {
      color: "var(--code-placeholder)",
      fontStyle: "italic",
    },
    ".cm-scroller": {
      overflow: "auto",
    },
  },
  { dark: false },
);

/* ------------------------------------------------------------------ */
/*  Dark theme                                                         */
/* ------------------------------------------------------------------ */
const darkTheme = EditorView.theme(
  {
    "&": {
      backgroundColor: "var(--code-bg)",
      color: "var(--code-text)",
      fontSize: "0.875rem",
      lineHeight: "1.625",
    },
    ".cm-content": {
      caretColor: "var(--code-caret)",
      fontFamily: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace",
      padding: "0.75rem 0",
    },
    ".cm-gutters": {
      backgroundColor: "var(--code-bg)",
      borderRight: "none",
      color: "var(--code-placeholder)",
    },
    ".cm-activeLineGutter": {
      backgroundColor: "transparent",
    },
    ".cm-activeLine": {
      backgroundColor: "transparent",
    },
    ".cm-cursor": {
      borderLeftColor: "var(--code-caret)",
    },
    ".cm-selectionBackground": {
      backgroundColor: "rgba(255,255,255,0.08) !important",
    },
    "&.cm-focused .cm-selectionBackground": {
      backgroundColor: "rgba(255,255,255,0.12) !important",
    },
    ".cm-placeholder": {
      color: "var(--code-placeholder)",
      fontStyle: "italic",
    },
    ".cm-scroller": {
      overflow: "auto",
    },
  },
  { dark: true },
);

/* ------------------------------------------------------------------ */
/*  Hook: returns the appropriate theme extension for the active mode   */
/* ------------------------------------------------------------------ */
function getIsDark(): boolean {
  return document.documentElement.classList.contains("dark");
}

export function useEditorTheme(): Extension {
  const [isDark, setIsDark] = useState(getIsDark);

  useEffect(() => {
    const observer = new MutationObserver(() => {
      setIsDark(getIsDark());
    });
    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ["class"],
    });
    return () => observer.disconnect();
  }, []);

  return useMemo(() => (isDark ? darkTheme : lightTheme), [isDark]);
}

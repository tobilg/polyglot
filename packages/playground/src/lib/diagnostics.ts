import type { Diagnostic } from "@codemirror/lint";

interface SpannedError {
  message: string;
  severity: "error" | "warning";
  start?: number;
  end?: number;
}

/**
 * Convert a UTF-8 byte offset to a JS string char index.
 * JS strings are UTF-16, so we encode to UTF-8 to get the byte mapping.
 */
export function byteOffsetToCharIndex(text: string, byteOffset: number): number {
  const encoder = new TextEncoder();
  const bytes = encoder.encode(text);
  // Decode the prefix of `byteOffset` bytes back to a string, then take its length.
  const decoder = new TextDecoder();
  const prefix = decoder.decode(bytes.slice(0, byteOffset));
  return prefix.length;
}

/**
 * Convert SDK ValidationErrors (with byte offsets) to CodeMirror Diagnostics.
 */
export function toCmDiagnostics(text: string, errors: SpannedError[]): Diagnostic[] {
  return errors
    .filter((e) => e.start != null && e.end != null)
    .map((e) => {
      const from = byteOffsetToCharIndex(text, e.start!);
      const to = byteOffsetToCharIndex(text, e.end!);
      return {
        from,
        to,
        severity: e.severity,
        message: e.message,
      } satisfies Diagnostic;
    });
}

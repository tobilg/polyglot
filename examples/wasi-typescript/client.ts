import {
  closeSync,
  mkdtempSync,
  openSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { WASI } from 'node:wasi';

export type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { [key: string]: JsonValue };
export type Ast = { [key: string]: JsonValue };
export type ParseRequest = { action: 'parse'; sql: string; dialect?: string };
export type SqlRequest =
  | { action: 'generate'; ast: Ast | Ast[]; dialect?: string }
  | {
      action: 'transpile';
      sql: string;
      read_dialect?: string;
      write_dialect?: string;
    }
  | { action: 'format'; sql: string; dialect?: string };
export type Request = ParseRequest | SqlRequest;
export type Response<T> =
  | { ok: true; result: T; error: null }
  | { ok: false; error: string };

export class SqlError extends Error {}

export function isObject(value: unknown): value is Ast {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

export class Client {
  private readonly compiled: WebAssembly.Module;

  private constructor(compiled: WebAssembly.Module) {
    this.compiled = compiled;
  }

  static async open(path: string | URL): Promise<Client> {
    return new Client(await WebAssembly.compile(readFileSync(path)));
  }

  async call(request: ParseRequest): Promise<Ast[]>;
  async call(request: SqlRequest): Promise<string[]>;
  async call(request: Request): Promise<Ast[] | string[]> {
    // node:wasi takes numeric file descriptors, not Node streams. Private
    // temporary files provide EOF on stdin and capture stdout/stderr reliably.
    const directory = mkdtempSync(join(tmpdir(), 'polyglot-wasi-'));
    const descriptors: number[] = [];
    const open = (name: string, flags: string): number => {
      const fd = openSync(join(directory, name), flags, 0o600);
      descriptors.push(fd);
      return fd;
    };
    try {
      writeFileSync(join(directory, 'stdin'), JSON.stringify(request), {
        mode: 0o600,
      });
      // Each call needs a new WASI object and instance; compiled code is reusable.
      const wasi = new WASI({
        version: 'preview1',
        args: [],
        env: {},
        preopens: {},
        returnOnExit: true,
        stdin: open('stdin', 'r'),
        stdout: open('stdout', 'w'),
        stderr: open('stderr', 'w'),
      });
      const instance = await WebAssembly.instantiate(this.compiled, {
        wasi_snapshot_preview1: wasi.wasiImport,
      });
      let exitCode: number;
      try {
        exitCode = wasi.start(instance);
      } catch (cause) {
        throw new Error(
          `WASI execution failed: ${readFileSync(join(directory, 'stderr'), 'utf8')}`,
          { cause },
        );
      }
      if (exitCode !== 0) {
        throw new Error(
          `WASI exited with ${exitCode}: ${readFileSync(join(directory, 'stderr'), 'utf8')}`,
        );
      }
      const response: unknown = JSON.parse(
        readFileSync(join(directory, 'stdout'), 'utf8'),
      );
      if (!isObject(response) || typeof response.ok !== 'boolean') {
        throw new Error('Invalid WASI response envelope');
      }
      if (!response.ok) {
        if (typeof response.error !== 'string')
          throw new Error('Missing WASI error message');
        throw new SqlError(response.error);
      }
      if (response.error !== null || !Array.isArray(response.result)) {
        throw new Error('Invalid WASI success response');
      }
      if (request.action === 'parse' && response.result.every(isObject))
        return response.result;
      if (
        request.action !== 'parse' &&
        response.result.every((sql): sql is string => typeof sql === 'string')
      ) {
        return response.result;
      }
      throw new Error(`Unexpected ${request.action} result shape`);
    } finally {
      // Close before removing files, including on Windows.
      for (const fd of descriptors) closeSync(fd);
      rmSync(directory, { recursive: true, force: true });
    }
  }
}

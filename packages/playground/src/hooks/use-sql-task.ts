import { useCallback, useEffect, useRef, useState, type DependencyList } from "react";
import { runSql } from "@/lib/sql-client";
import type { SqlOperations } from "@/lib/sql-operations";

// Ignore responses for edited inputs, superseded runs, or unmounted tabs.
export function useSqlTask(inputs: DependencyList) {
  const generation = useRef(0);
  const [pending, setPending] = useState(false);
  useEffect(() => {
    generation.current++;
    setPending(false);
    return () => { generation.current++; };
  }, inputs);
  const run = useCallback(async <K extends keyof SqlOperations>(
    method: K, ...args: Parameters<SqlOperations[K]>
  ): Promise<ReturnType<SqlOperations[K]> | undefined> => {
    const id = ++generation.current;
    setPending(true);
    try {
      const result = await runSql(method, ...args);
      return id === generation.current ? result : undefined;
    } catch (error) {
      if (id === generation.current) throw error;
      return undefined;
    } finally {
      if (id === generation.current) setPending(false);
    }
  }, []);
  return { run, pending };
}

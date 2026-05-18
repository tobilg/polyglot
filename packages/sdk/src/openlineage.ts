/**
 * OpenLineage-compatible lineage payload generation.
 *
 * These helpers only produce JSON-compatible OpenLineage payloads. They do not
 * send events or manage any OpenLineage transport/client lifecycle.
 */

import {
  openlineage_column_lineage as wasmOpenLineageColumnLineage,
  openlineage_job_event as wasmOpenLineageJobEvent,
  openlineage_run_event as wasmOpenLineageRunEvent,
} from '../wasm/polyglot_sql_wasm.js';
import type { Schema } from './validation/schema';

export interface OpenLineageDatasetId {
  namespace: string;
  name: string;
}

export type OpenLineageRunEventType =
  | 'START'
  | 'RUNNING'
  | 'COMPLETE'
  | 'ABORT'
  | 'FAIL'
  | 'OTHER';

export interface OpenLineageOptions {
  dialect?: string;
  producer: string;
  datasetNamespace?: string;
  datasetMappings?: Record<string, OpenLineageDatasetId>;
  outputDataset?: OpenLineageDatasetId;
  schema?: Schema;
  jobNamespace?: string;
  jobName?: string;
  eventTime?: string;
  runId?: string;
  eventType?: OpenLineageRunEventType;
}

export interface OpenLineageWarning {
  code: string;
  message: string;
}

export interface OpenLineageTransformation {
  type: 'DIRECT' | string;
  subtype: 'IDENTITY' | 'TRANSFORMATION' | 'AGGREGATION' | string;
  description?: string;
  masking?: boolean;
}

export interface OpenLineageInputField {
  namespace: string;
  name: string;
  field: string;
  transformations?: OpenLineageTransformation[];
}

export interface OpenLineageColumnLineageField {
  inputFields: OpenLineageInputField[];
}

export interface OpenLineageColumnLineageFacet {
  _producer: string;
  _schemaURL: string;
  fields: Record<string, OpenLineageColumnLineageField>;
}

export interface OpenLineageDataset {
  namespace: string;
  name: string;
  facets?: Record<string, unknown>;
}

export interface OpenLineageColumnLineageResult {
  success: boolean;
  facet?: OpenLineageColumnLineageFacet;
  inputs?: OpenLineageDataset[];
  outputs?: OpenLineageDataset[];
  warnings: OpenLineageWarning[];
  error?: string;
}

export interface OpenLineageEventResult {
  success: boolean;
  event?: Record<string, unknown>;
  warnings: OpenLineageWarning[];
  error?: string;
}

function normalizeOptions(options: OpenLineageOptions): OpenLineageOptions {
  return {
    dialect: 'generic',
    datasetMappings: {},
    ...options,
  };
}

/**
 * Build a standalone OpenLineage columnLineage facet with input/output datasets.
 */
export function openLineageColumnLineage(
  sql: string,
  options: OpenLineageOptions,
): OpenLineageColumnLineageResult {
  const resultJson = wasmOpenLineageColumnLineage(
    sql,
    JSON.stringify(normalizeOptions(options)),
  );
  return JSON.parse(resultJson) as OpenLineageColumnLineageResult;
}

/**
 * Build an OpenLineage JobEvent payload. This does not send the event.
 */
export function openLineageJobEvent(
  sql: string,
  options: OpenLineageOptions,
): OpenLineageEventResult {
  const resultJson = wasmOpenLineageJobEvent(
    sql,
    JSON.stringify(normalizeOptions(options)),
  );
  return JSON.parse(resultJson) as OpenLineageEventResult;
}

/**
 * Build an OpenLineage RunEvent payload. This does not send the event.
 */
export function openLineageRunEvent(
  sql: string,
  options: OpenLineageOptions,
): OpenLineageEventResult {
  const resultJson = wasmOpenLineageRunEvent(
    sql,
    JSON.stringify(normalizeOptions(options)),
  );
  return JSON.parse(resultJson) as OpenLineageEventResult;
}

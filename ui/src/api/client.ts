import type {
  Project,
  ConnectorInfo,
  Table,
  Settings,
  MigrationRun,
  ConnectionConfig,
} from '../types';

function getAuthToken(): string | null {
  return sessionStorage.getItem('bani_auth_token');
}

async function request<T>(
  path: string,
  options: RequestInit = {},
): Promise<T> {
  const token = getAuthToken();
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...(options.headers as Record<string, string> | undefined),
  };
  if (token) {
    headers['Authorization'] = `Bearer ${token}`;
  }

  const response = await fetch(path, {
    ...options,
    headers,
  });

  if (!response.ok) {
    if (response.status === 401) {
      // Token invalid or missing — clear it so login page shows
      sessionStorage.removeItem('bani_auth_token');
      window.location.reload();
      throw new Error('Session expired. Please log in again.');
    }
    const body = await response.text();
    throw new Error(`API error ${response.status}: ${body}`);
  }

  if (response.status === 204) {
    return undefined as T;
  }

  return response.json() as Promise<T>;
}

// --- Projects ---

export interface ProjectSummary {
  name: string;
  path: string;
}

export function getProjects(): Promise<ProjectSummary[]> {
  return request<ProjectSummary[]>('/api/projects');
}

export function getProject(id: string): Promise<Project> {
  return request<Project>(`/api/projects/${id}`);
}

/**
 * Generate the credential BDL element.
 *
 * - Env var mode: `${env:VAR_NAME}` — resolved at runtime.
 * - Direct mode: generate a descriptive env var name like
 *   `BANI_<ROLE>_USER` so the BDL never contains raw secrets.
 *   The user must set these env vars before running the migration.
 */
export function createProject(
  data: { name: string; content: string },
): Promise<Project> {
  return request<Project>('/api/projects', {
    method: 'POST',
    body: JSON.stringify(data),
  });
}

export function updateProject(
  id: string,
  project: Partial<Project>,
): Promise<Project> {
  return request<Project>(`/api/projects/${id}`, {
    method: 'PUT',
    body: JSON.stringify(project),
  });
}

export function deleteProject(id: string): Promise<void> {
  return request<void>(`/api/projects/${id}`, {
    method: 'DELETE',
  });
}

// --- Migrations ---

export function startMigration(projectId: string): Promise<{ run_id: string }> {
  return request<{ run_id: string }>(`/api/projects/${projectId}/run`, {
    method: 'POST',
  });
}

export function validateMigration(
  projectName: string,
): Promise<{ source: string; target: string }> {
  return request<{ source: string; target: string }>('/api/migrate/validate', {
    method: 'POST',
    body: JSON.stringify({ project_name: projectName }),
  });
}

export interface DryRunTable {
  name: string;
  columns: number;
  estimated_rows: number | null;
}

export interface DryRunResult {
  status: string;
  dry_run: true;
  project_name: string;
  source_dialect: string;
  target_dialect: string;
  table_count: number;
  total_estimated_rows: number;
  tables: DryRunTable[];
}

export interface MigrationStartResult {
  status: string;
  project_name: string;
}

export function startMigrationRun(
  projectName: string,
  options?: { resume?: boolean; dry_run?: boolean },
): Promise<MigrationStartResult | DryRunResult> {
  return request<MigrationStartResult | DryRunResult>('/api/migrate', {
    method: 'POST',
    body: JSON.stringify({ project_name: projectName, ...options }),
  });
}

export function getMigrationStatus(runId: string): Promise<MigrationRun> {
  return request<MigrationRun>(`/api/runs/${runId}`);
}

export function getMigrationRuns(): Promise<MigrationRun[]> {
  return request<MigrationRun[]>('/api/runs');
}

export interface RunSummary {
  total_runs: number;
  last_run: RunLogEntry | null;
  lifetime_rows: number;
}

export interface RunLogEntry {
  project_name: string;
  started_at: string;
  finished_at: string;
  status: string;
  tables_completed: number;
  tables_failed: number;
  total_rows: number;
  duration_seconds: number;
  error: string | null;
}

export function getRunHistory(_n: number = 50): Promise<RunLogEntry[]> {
  return request<RunLogEntry[]>('/api/runs');
}

export function getRunSummary(): Promise<RunSummary> {
  return request<RunSummary>('/api/runs/summary');
}

export function getLastRunPerProject(): Promise<Record<string, RunLogEntry>> {
  return request<Record<string, RunLogEntry>>('/api/runs/last-per-project');
}

export function clearRunHistory(): Promise<{ detail: string }> {
  return request<{ detail: string }>('/api/runs', { method: 'DELETE' });
}

export interface ScheduleInfo {
  project: string;
  cron: string;
  next_run: string | null;
  status: 'active' | 'failed';
}

export function getSchedules(): Promise<ScheduleInfo[]> {
  return request<ScheduleInfo[]>('/api/schedules');
}

export interface MigrateStatusResponse {
  running: boolean;
  phase: string | null;
  project_name: string | null;
  tables_completed: number;
  tables_failed: number;
  total_tables: number;
  total_rows_read: number;
  total_rows_written: number;
  error: string | null;
  current_table: string | null;
  table_failures: string[];
  warnings: string[];
  elapsed_seconds: number;
}

export function getMigrateStatus(): Promise<MigrateStatusResponse> {
  return request<MigrateStatusResponse>('/api/migrate/status');
}

export function cancelMigration(): Promise<{ detail: string }> {
  return request<{ detail: string }>('/api/migrate/cancel', { method: 'POST' });
}

export interface CheckpointInfo {
  exists: boolean;
  tables_completed?: number;
  tables_total?: number;
  created_at?: string;
}

export function getCheckpoint(projectName: string): Promise<CheckpointInfo> {
  return request<CheckpointInfo>(`/api/migrate/checkpoint/${encodeURIComponent(projectName)}`);
}

export function deleteCheckpoint(projectName: string): Promise<{ detail: string }> {
  return request<{ detail: string }>(`/api/migrate/checkpoint/${encodeURIComponent(projectName)}`, {
    method: 'DELETE',
  });
}

// --- Schema Inspection ---

interface ApiTable {
  schema_name: string;
  table_name: string;
  columns: Array<{
    name: string;
    data_type: string;
    nullable: boolean;
    default_value: string | null;
    is_auto_increment: boolean;
    arrow_type_str: string | null;
  }>;
  primary_key: string[];
  indexes: Array<{ name: string; columns: string[]; is_unique: boolean }>;
  foreign_keys: Array<{
    name: string;
    source_table: string;
    source_columns: string[];
    referenced_table: string;
    referenced_columns: string[];
  }>;
  row_count_estimate: number | null;
}

export async function inspectSchema(
  connection: ConnectionConfig,
): Promise<Table[]> {
  const result = await request<{ source_dialect: string; tables: ApiTable[] }>(
    '/api/schema/inspect',
    {
      method: 'POST',
      body: JSON.stringify(connection),
    },
  );
  // Transform API shape to frontend Table shape
  return result.tables.map((t) => {
    const pkSet = new Set(t.primary_key);
    return {
      schema_name: t.schema_name,
      name: t.table_name,
      columns: t.columns.map((c) => ({
        name: c.name,
        data_type: c.data_type,
        nullable: c.nullable,
        default_value: c.default_value,
        is_primary_key: pkSet.has(c.name),
        arrow_type_str: c.arrow_type_str,
      })),
      indexes: t.indexes,
      foreign_keys: t.foreign_keys.map((fk) => ({
        name: fk.name,
        columns: fk.source_columns,
        referenced_table: fk.referenced_table,
        referenced_schema: null,
        referenced_columns: fk.referenced_columns,
      })),
      row_count: t.row_count_estimate,
    };
  });
}

// --- Connections (registry) ---

export interface RegisteredConnectionSummary {
  key: string;
  name: string;
  connector: string;
  host: string;
  port: number;
  database: string;
}

export interface ConnectionsResponse {
  connections: Record<string, RegisteredConnectionSummary>;
  count: number;
}

export function getConnections(): Promise<ConnectionsResponse> {
  return request<ConnectionsResponse>('/api/connections');
}

export function getConnectionConfig(key: string): Promise<ConnectionConfig> {
  return request<ConnectionConfig>(`/api/connections/${encodeURIComponent(key)}/config`);
}

// --- Connectors ---

export function getConnectors(): Promise<ConnectorInfo[]> {
  return request<ConnectorInfo[]>('/api/connectors');
}

export function getConnectorInfo(name: string): Promise<ConnectorInfo> {
  return request<ConnectorInfo>(`/api/connectors/${name}`);
}

// --- Settings ---

export function getSettings(): Promise<Settings> {
  return request<Settings>('/api/settings');
}

export function updateSettings(settings: Partial<Settings>): Promise<Settings> {
  return request<Settings>('/api/settings', {
    method: 'PUT',
    body: JSON.stringify(settings),
  });
}

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
    const body = await response.text();
    throw new Error(`API error ${response.status}: ${body}`);
  }

  if (response.status === 204) {
    return undefined as T;
  }

  return response.json() as Promise<T>;
}

// --- Projects ---

export function getProjects(): Promise<Project[]> {
  return request<Project[]>('/api/projects');
}

export function getProject(id: string): Promise<Project> {
  return request<Project>(`/api/projects/${id}`);
}

export function createProject(
  project: Omit<Project, 'id' | 'status' | 'created_at' | 'updated_at'>,
): Promise<Project> {
  return request<Project>('/api/projects', {
    method: 'POST',
    body: JSON.stringify(project),
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

export function getMigrationStatus(runId: string): Promise<MigrationRun> {
  return request<MigrationRun>(`/api/runs/${runId}`);
}

export function getMigrationRuns(): Promise<MigrationRun[]> {
  return request<MigrationRun[]>('/api/runs');
}

// --- Schema Inspection ---

export function inspectSchema(
  connection: ConnectionConfig,
): Promise<Table[]> {
  return request<Table[]>('/api/schema/inspect', {
    method: 'POST',
    body: JSON.stringify(connection),
  });
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

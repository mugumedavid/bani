/** Core domain types for the Bani Web UI. */

export interface Column {
  name: string;
  data_type: string;
  nullable: boolean;
  default_value: string | null;
  is_primary_key: boolean;
  arrow_type_str: string | null;
}

export interface Index {
  name: string;
  columns: string[];
  is_unique: boolean;
}

export interface ForeignKey {
  name: string;
  columns: string[];
  referenced_table: string;
  referenced_schema: string | null;
  referenced_columns: string[];
}

export interface Table {
  schema_name: string | null;
  name: string;
  columns: Column[];
  indexes: Index[];
  foreign_keys: ForeignKey[];
  row_count: number | null;
}

export interface ConnectorInfo {
  name: string;
  display_name: string;
  version: string;
  description: string;
  supported_databases: string[];
  driver: string;
  status: 'stable' | 'beta' | 'planned';
}

export interface ConnectionConfig {
  connector: string;
  host: string;
  port: number;
  database: string;
  username_env: string;
  password_env: string;
  extra: Record<string, string>;
}

export interface TypeMappingOverride {
  source_type: string;
  target_type: string;
}

export interface Hook {
  phase: 'pre_migration' | 'post_migration' | 'pre_table' | 'post_table';
  command: string;
  type: 'sql' | 'shell';
}

export interface Project {
  id: string;
  name: string;
  description: string;
  source: ConnectionConfig;
  target: ConnectionConfig;
  tables: string[];
  type_mapping_overrides: TypeMappingOverride[];
  hooks: Hook[];
  schedule: string | null;
  status: ProjectStatus;
  created_at: string;
  updated_at: string;
}

export type ProjectStatus = 'idle' | 'running' | 'completed' | 'failed';

export interface MigrationEvent {
  type: 'table_start' | 'table_progress' | 'table_complete' | 'table_error' | 'migration_complete' | 'migration_error';
  project_id: string;
  table_name: string | null;
  rows_transferred: number;
  total_rows: number;
  throughput: number;
  eta_seconds: number | null;
  error: string | null;
  timestamp: string;
}

export interface MigrationRun {
  id: string;
  project_id: string;
  project_name: string;
  status: ProjectStatus;
  started_at: string;
  completed_at: string | null;
  duration_seconds: number | null;
  tables_total: number;
  tables_completed: number;
  rows_total: number;
  rows_transferred: number;
  errors: string[];
  checkpoint_file: string | null;
}

export interface TableProgress {
  table_name: string;
  rows_transferred: number;
  total_rows: number;
  status: 'pending' | 'running' | 'completed' | 'error';
  error: string | null;
  throughput: number;
}

export interface MigrationProgress {
  project_id: string;
  status: ProjectStatus;
  tables: TableProgress[];
  overall_rows_transferred: number;
  overall_total_rows: number;
  throughput: number;
  eta_seconds: number | null;
  errors: string[];
}

export interface Settings {
  batch_size: number;
  max_workers: number;
  memory_limit_mb: number;
  log_level: 'DEBUG' | 'INFO' | 'WARNING' | 'ERROR';
  checkpoint_enabled: boolean;
  checkpoint_dir: string;
}

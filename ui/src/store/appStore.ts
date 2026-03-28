import { create } from 'zustand';
import type { MigrationEvent } from '../types';

interface TableProgressData {
  table_name: string;
  rows_transferred: number;
  total_rows: number;
  throughput: number;
}

interface AppState {
  // Auth
  authToken: string | null;
  setAuthToken: (token: string | null) => void;

  // Migration real-time state
  migrationEvents: MigrationEvent[];
  addMigrationEvent: (event: MigrationEvent) => void;
  clearMigrationEvents: () => void;

  // Per-table progress map: projectId -> tableName -> progress
  tableProgress: Record<string, Record<string, TableProgressData>>;
  setMigrationProgress: (projectId: string, data: TableProgressData) => void;
  clearMigrationProgress: (projectId: string) => void;
}

export const useAppStore = create<AppState>((set) => ({
  // Auth
  authToken: sessionStorage.getItem('bani_auth_token'),
  setAuthToken: (token) => set({ authToken: token }),

  // Migration events
  migrationEvents: [],
  addMigrationEvent: (event) =>
    set((state) => ({
      migrationEvents: [...state.migrationEvents.slice(-500), event],
    })),
  clearMigrationEvents: () => set({ migrationEvents: [] }),

  // Table progress
  tableProgress: {},
  setMigrationProgress: (projectId, data) =>
    set((state) => ({
      tableProgress: {
        ...state.tableProgress,
        [projectId]: {
          ...state.tableProgress[projectId],
          [data.table_name]: data,
        },
      },
    })),
  clearMigrationProgress: (projectId) =>
    set((state) => {
      const next = { ...state.tableProgress };
      delete next[projectId];
      return { tableProgress: next };
    }),
}));

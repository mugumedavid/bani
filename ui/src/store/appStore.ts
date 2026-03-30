import { create } from 'zustand';
import type { MigrationEvent, TableEstimate } from '../types';

export interface TableProgressData {
  table_name: string;
  rows_transferred: number;
  total_rows: number;
  status: 'pending' | 'running' | 'completed' | 'failed';
}

interface AppState {
  // Auth
  authToken: string | null;
  setAuthToken: (token: string | null) => void;

  // Migration real-time state
  migrationEvents: MigrationEvent[];
  addMigrationEvent: (event: MigrationEvent) => void;
  clearMigrationEvents: () => void;

  // Per-table progress map: tableName -> progress
  tableProgress: Record<string, TableProgressData>;
  initTableProgress: (tables: TableEstimate[]) => void;
  updateTableStatus: (tableName: string, status: TableProgressData['status']) => void;
  addTableRows: (tableName: string, rows: number) => void;
  setTableComplete: (tableName: string, totalRows: number) => void;
  clearTableProgress: () => void;
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

  initTableProgress: (tables) =>
    set(() => {
      const progress: Record<string, TableProgressData> = {};
      for (const t of tables) {
        progress[t.name] = {
          table_name: t.name,
          rows_transferred: 0,
          total_rows: t.estimated_rows ?? 0,
          status: 'pending',
        };
      }
      return { tableProgress: progress };
    }),

  updateTableStatus: (tableName, status) =>
    set((state) => {
      const existing = state.tableProgress[tableName];
      if (!existing) return state;
      return {
        tableProgress: {
          ...state.tableProgress,
          [tableName]: { ...existing, status },
        },
      };
    }),

  addTableRows: (tableName, rows) =>
    set((state) => {
      const existing = state.tableProgress[tableName];
      if (!existing) return state;
      return {
        tableProgress: {
          ...state.tableProgress,
          [tableName]: {
            ...existing,
            rows_transferred: existing.rows_transferred + rows,
          },
        },
      };
    }),

  setTableComplete: (tableName, totalRows) =>
    set((state) => {
      const existing = state.tableProgress[tableName];
      if (!existing) return state;
      return {
        tableProgress: {
          ...state.tableProgress,
          [tableName]: {
            ...existing,
            rows_transferred: totalRows,
            status: 'completed',
          },
        },
      };
    }),

  clearTableProgress: () => set({ tableProgress: {} }),
}));

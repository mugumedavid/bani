import { useEffect, useRef } from 'react';
import { useAppStore } from '../store/appStore';
import type { MigrationEvent } from '../types';

/**
 * Subscribe to the SSE progress stream while a migration is running.
 *
 * Uses the native EventSource API which auto-reconnects on disconnect.
 * Events are pushed to the Zustand store so any component can consume them.
 */
export function useSSE(enabled: boolean): void {
  const sourceRef = useRef<EventSource | null>(null);
  const addEvent = useAppStore((s) => s.addMigrationEvent);
  const initTableProgress = useAppStore((s) => s.initTableProgress);
  const updateTableStatus = useAppStore((s) => s.updateTableStatus);
  const addTableRows = useAppStore((s) => s.addTableRows);
  const setTableComplete = useAppStore((s) => s.setTableComplete);

  useEffect(() => {
    if (!enabled) {
      if (sourceRef.current) {
        sourceRef.current.close();
        sourceRef.current = null;
      }
      return;
    }

    const token = sessionStorage.getItem('bani_auth_token');
    const url = `/api/migrate/progress${token ? `?token=${token}` : ''}`;
    const source = new EventSource(url);
    sourceRef.current = source;

    source.onmessage = (e) => {
      const event: MigrationEvent = JSON.parse(e.data);
      addEvent(event);

      switch (event.type) {
        case 'introspection_complete':
          if (event.tables) {
            initTableProgress(event.tables);
          }
          break;
        case 'table_start':
          if (event.table_name) {
            updateTableStatus(event.table_name, 'running');
          }
          break;
        case 'batch_complete':
          if (event.table_name && event.rows_written) {
            addTableRows(event.table_name, event.rows_written);
          }
          break;
        case 'table_complete':
          if (event.table_name) {
            setTableComplete(
              event.table_name,
              event.total_rows_written ?? 0,
            );
          }
          break;
        case 'table_create_failed':
          if (event.table_name) {
            updateTableStatus(event.table_name, 'failed');
          }
          break;
      }
    };

    return () => {
      source.close();
      sourceRef.current = null;
    };
  }, [enabled, addEvent, initTableProgress, updateTableStatus, addTableRows, setTableComplete]);
}

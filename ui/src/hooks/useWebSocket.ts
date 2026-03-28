import { useEffect, useRef, useCallback } from 'react';
import type { MigrationEvent } from '../types';
import { useAppStore } from '../store/appStore';

export function useWebSocket(projectId: string | null) {
  const wsRef = useRef<WebSocket | null>(null);
  const addEvent = useAppStore((s) => s.addMigrationEvent);
  const setProgress = useAppStore((s) => s.setMigrationProgress);
  const token = useAppStore((s) => s.authToken);

  const connect = useCallback(() => {
    if (!projectId) return;

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const url = `${protocol}//${window.location.host}/ws/migration/${projectId}${token ? `?token=${token}` : ''}`;

    const ws = new WebSocket(url);
    wsRef.current = ws;

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data) as MigrationEvent;
        addEvent(data);

        if (data.type === 'table_progress' || data.type === 'table_complete') {
          setProgress(data.project_id, {
            table_name: data.table_name ?? '',
            rows_transferred: data.rows_transferred,
            total_rows: data.total_rows,
            throughput: data.throughput,
          });
        }
      } catch {
        // Ignore malformed messages
      }
    };

    ws.onclose = () => {
      wsRef.current = null;
    };

    return ws;
  }, [projectId, token, addEvent, setProgress]);

  useEffect(() => {
    const ws = connect();
    return () => {
      ws?.close();
    };
  }, [connect]);

  const disconnect = useCallback(() => {
    wsRef.current?.close();
    wsRef.current = null;
  }, []);

  return { disconnect };
}

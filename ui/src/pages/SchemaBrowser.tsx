import { useState, useEffect, useRef } from 'react';
import { useMutation, useQuery } from '@tanstack/react-query';
import { inspectSchema, getConnectors, getConnections, getConnectionConfig } from '../api/client';
import type { RegisteredConnectionSummary } from '../api/client';
import { SchemaTree } from '../components/SchemaTree';
import { ConnectionForm } from '../components/ConnectionForm';
import type { ConnectionConfig, Table, SavedConnection } from '../types';

const CONNECTIONS_KEY = 'bani_saved_connections';
const ACTIVE_KEY = 'bani_active_connection';

const emptyConnection: ConnectionConfig = {
  name: '',
  connector: '',
  host: 'localhost',
  port: 5432,
  database: '',
  username_env: '',
  password_env: '',
  username_is_env: false,
  password_is_env: false,
  extra: {},
};

function loadSavedConnections(): SavedConnection[] {
  try {
    const raw = sessionStorage.getItem(CONNECTIONS_KEY);
    if (raw) return JSON.parse(raw);
  } catch { /* ignore */ }
  return [];
}

function saveSavedConnections(connections: SavedConnection[]) {
  sessionStorage.setItem(CONNECTIONS_KEY, JSON.stringify(connections));
}

function loadActiveState(): { connection: ConnectionConfig; tables: Table[] | null } {
  try {
    const raw = sessionStorage.getItem(ACTIVE_KEY);
    if (raw) return JSON.parse(raw);
  } catch { /* ignore */ }
  return { connection: { ...emptyConnection }, tables: null };
}

function saveActiveState(connection: ConnectionConfig, tables: Table[] | null) {
  sessionStorage.setItem(ACTIVE_KEY, JSON.stringify({ connection, tables }));
}

export function SchemaBrowser() {
  const saved = loadActiveState();
  const [connection, setConnection] = useState<ConnectionConfig>(saved.connection);
  const [tables, setTables] = useState<Table[] | null>(saved.tables);
  const [savedConnections, setSavedConnections] = useState<SavedConnection[]>(loadSavedConnections);

  const { data: connectors } = useQuery({
    queryKey: ['connectors'],
    queryFn: getConnectors,
  });

  const { data: registry } = useQuery({
    queryKey: ['connections'],
    queryFn: getConnections,
    staleTime: 30_000,
  });

  const registeredConnections: RegisteredConnectionSummary[] = registry
    ? Object.values(registry.connections)
    : [];

  const connectorNames = connectors?.map((c) => c.name) ?? [
    'postgresql', 'mysql', 'mssql', 'oracle', 'sqlite',
  ];

  async function loadRegisteredConnection(key: string) {
    try {
      const config = await getConnectionConfig(key);
      setConnection(config);
      setTables(null);
    } catch {
      // Silently ignore
    }
  }

  // Persist active state
  useEffect(() => {
    saveActiveState(connection, tables);
  }, [connection, tables]);

  const [elapsed, setElapsed] = useState(0);
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const inspectMutation = useMutation({
    mutationFn: () => {
      abortRef.current = new AbortController();
      return inspectSchema(connection);
    },
    onMutate: () => {
      setElapsed(0);
      timerRef.current = setInterval(() => setElapsed((e) => e + 1), 1000);
    },
    onSuccess: (data) => {
      setTables(data);
      if (connection.name.trim()) {
        saveConnection(connection, data);
      }
    },
    onSettled: () => {
      if (timerRef.current) {
        clearInterval(timerRef.current);
        timerRef.current = null;
      }
    },
  });

  // Timeout after 120 seconds
  useEffect(() => {
    if (inspectMutation.isPending && elapsed >= 120) {
      abortRef.current?.abort();
      inspectMutation.reset();
    }
  }, [elapsed, inspectMutation]);

  function saveConnection(conn: ConnectionConfig, tbl: Table[] | null) {
    const updated = savedConnections.filter((s) => s.connection.name !== conn.name);
    updated.unshift({
      connection: conn,
      tables: tbl,
      savedAt: new Date().toISOString(),
    });
    setSavedConnections(updated);
    saveSavedConnections(updated);
  }

  function loadConnection(saved: SavedConnection) {
    setConnection(saved.connection);
    setTables(saved.tables);
  }


  function clearForm() {
    setConnection({ ...emptyConnection });
    setTables(null);
  }

  return (
    <div>
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-gray-900">Schema Browser</h1>
        <p className="text-gray-500 mt-1">
          Connect to a database and inspect its schema
        </p>
      </div>

      {/* Connection Picker */}
      {(registeredConnections.length > 0 || savedConnections.length > 0) && (
        <div className="flex items-center justify-between mb-6">
          <select
            value=""
            onChange={async (e) => {
              const val = e.target.value;
              if (!val) return;
              if (val.startsWith('reg:')) {
                await loadRegisteredConnection(val.slice(4));
              } else {
                const found = savedConnections.find((s) => s.connection.name === val);
                if (found) loadConnection(found);
              }
            }}
            className="w-80 h-9 rounded-md border border-gray-300 px-3 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500"
          >
            <option value="">Select a connection...</option>
            {registeredConnections.length > 0 && (
              <optgroup label="Registered">
                {registeredConnections.map((r: RegisteredConnectionSummary) => (
                  <option key={`reg:${r.key}`} value={`reg:${r.key}`}>
                    {r.name} ({r.connector} — {r.host}:{r.port}/{r.database})
                  </option>
                ))}
              </optgroup>
            )}
            {savedConnections.length > 0 && (
              <optgroup label="Session Saved">
                {savedConnections.map((s) => (
                  <option key={s.connection.name} value={s.connection.name}>
                    {s.connection.name} ({s.connection.connector} — {s.connection.host}:{s.connection.port}/{s.connection.database})
                  </option>
                ))}
              </optgroup>
            )}
          </select>
          <button
            onClick={clearForm}
            className="h-9 rounded-md border border-gray-300 px-4 text-sm text-gray-600 hover:bg-gray-50"
          >
            Clear
          </button>
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
        {/* Connection Form */}
        <div className="bg-white rounded-xl border border-gray-200 p-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">
            Connection
          </h2>
          <ConnectionForm
            value={connection}
            onChange={setConnection}
            connectorOptions={connectorNames}
          />
          <div className="mt-6 flex gap-3 items-center">
            <button
              onClick={() => inspectMutation.mutate()}
              disabled={inspectMutation.isPending || !connection.connector}
              className="inline-flex items-center gap-2 px-4 py-2.5 bg-indigo-600 text-white text-sm font-medium rounded-lg hover:bg-indigo-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
            >
              {inspectMutation.isPending ? (
                <>
                  <svg className="animate-spin w-4 h-4" fill="none" viewBox="0 0 24 24">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                  </svg>
                  Inspecting... {elapsed}s
                </>
              ) : (
                <>
                  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
                  </svg>
                  Inspect Schema
                </>
              )}
            </button>
            {inspectMutation.isPending && (
              <button
                onClick={() => {
                  abortRef.current?.abort();
                  inspectMutation.reset();
                }}
                className="px-3 py-2 text-red-600 text-sm font-medium rounded-lg hover:bg-red-50 transition-colors"
              >
                Cancel
              </button>
            )}
            {!inspectMutation.isPending && (
              <button
                onClick={clearForm}
                className="px-4 py-2.5 text-gray-600 text-sm font-medium rounded-lg hover:bg-gray-100 transition-colors"
              >
                New Connection
              </button>
            )}
          </div>

          {inspectMutation.isError && (
            <div className="mt-4 bg-red-50 border border-red-200 rounded-lg p-4">
              <p className="text-sm text-red-700">
                {(inspectMutation.error as Error).message}
              </p>
            </div>
          )}
        </div>

        {/* Schema Tree */}
        <div>
          <h2 className="text-lg font-semibold text-gray-900 mb-4">
            Schema
            {tables !== null && connection.name && (
              <span className="text-sm font-normal text-gray-400 ml-2">
                — {connection.name}
              </span>
            )}
          </h2>
          {tables === null ? (
            <div className="bg-white rounded-xl border border-gray-200 p-8 text-center text-gray-400">
              <svg className="w-12 h-12 mx-auto mb-3 text-gray-300" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1} d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4" />
              </svg>
              <p>Connect to a database to browse its schema</p>
            </div>
          ) : (
            <SchemaTree tables={tables} />
          )}
        </div>
      </div>
    </div>
  );
}

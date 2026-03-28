import { useState } from 'react';
import { useMutation, useQuery } from '@tanstack/react-query';
import { inspectSchema, getConnectors } from '../api/client';
import { SchemaTree } from '../components/SchemaTree';
import { ConnectionForm } from '../components/ConnectionForm';
import type { ConnectionConfig, Table } from '../types';

const emptyConnection: ConnectionConfig = {
  connector: '',
  host: 'localhost',
  port: 5432,
  database: '',
  username_env: '',
  password_env: '',
  extra: {},
};

export function SchemaBrowser() {
  const [connection, setConnection] = useState<ConnectionConfig>({
    ...emptyConnection,
  });
  const [tables, setTables] = useState<Table[] | null>(null);

  const { data: connectors } = useQuery({
    queryKey: ['connectors'],
    queryFn: getConnectors,
  });

  const connectorNames = connectors?.map((c) => c.name) ?? [
    'postgresql',
    'mysql',
    'mssql',
    'oracle',
    'sqlite',
  ];

  const inspectMutation = useMutation({
    mutationFn: () => inspectSchema(connection),
    onSuccess: (data) => setTables(data),
  });

  return (
    <div>
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-gray-900">Schema Browser</h1>
        <p className="text-gray-500 mt-1">
          Connect to a database and inspect its schema
        </p>
      </div>

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
          <div className="mt-6">
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
                  Inspecting...
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
          <h2 className="text-lg font-semibold text-gray-900 mb-4">Schema</h2>
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

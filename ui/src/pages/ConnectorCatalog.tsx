import { useQuery } from '@tanstack/react-query';
import { getConnectors } from '../api/client';
import type { ConnectorInfo } from '../types';

const statusBadge: Record<string, string> = {
  stable: 'bg-green-100 text-green-700',
  beta: 'bg-yellow-100 text-yellow-700',
  planned: 'bg-gray-100 text-gray-500',
};

const dbIcons: Record<string, string> = {
  postgresql: 'PG',
  mysql: 'My',
  mssql: 'MS',
  oracle: 'Or',
  sqlite: 'SQ',
};

export function ConnectorCatalog() {
  const { data: connectors, isLoading, error } = useQuery({
    queryKey: ['connectors'],
    queryFn: getConnectors,
  });

  return (
    <div>
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-gray-900">Connector Catalog</h1>
        <p className="text-gray-500 mt-1">
          Installed database connectors and their capabilities
        </p>
      </div>

      {isLoading && (
        <div className="flex items-center justify-center py-16">
          <div className="flex items-center gap-3 text-gray-500">
            <svg className="animate-spin w-5 h-5" fill="none" viewBox="0 0 24 24">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
            </svg>
            Loading connectors...
          </div>
        </div>
      )}

      {error && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4">
          <p className="text-sm text-red-700">
            Failed to load connectors: {(error as Error).message}
          </p>
        </div>
      )}

      {connectors && connectors.length === 0 && (
        <div className="text-center py-16 bg-white rounded-xl border border-gray-200">
          <h3 className="text-lg font-medium text-gray-900 mb-1">
            No connectors installed
          </h3>
          <p className="text-gray-500">
            Install connectors via pip to see them here.
          </p>
        </div>
      )}

      {connectors && connectors.length > 0 && (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {connectors.map((connector) => (
            <ConnectorCard key={connector.name} connector={connector} />
          ))}
        </div>
      )}
    </div>
  );
}

function ConnectorCard({ connector }: { connector: ConnectorInfo }) {
  const abbr = dbIcons[connector.name] ?? connector.name.slice(0, 2).toUpperCase();

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6 hover:shadow-md transition-shadow">
      <div className="flex items-start gap-4 mb-4">
        <div className="w-12 h-12 bg-indigo-100 text-indigo-700 rounded-lg flex items-center justify-center text-lg font-bold flex-shrink-0">
          {abbr}
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <h3 className="font-semibold text-gray-900 truncate">
              {connector.display_name}
            </h3>
            <span
              className={`px-2 py-0.5 rounded-full text-xs font-medium ${statusBadge[connector.status] ?? statusBadge.planned}`}
            >
              {connector.status}
            </span>
          </div>
          <p className="text-sm text-gray-500 mt-1">{connector.description}</p>
        </div>
      </div>

      <div className="space-y-2 text-sm">
        <div className="flex items-center justify-between">
          <span className="text-gray-500">Version</span>
          <span className="font-mono text-gray-700">{connector.version}</span>
        </div>
        <div className="flex items-center justify-between">
          <span className="text-gray-500">Driver</span>
          <span className="font-mono text-gray-700">{connector.driver}</span>
        </div>
        {connector.supported_databases.length > 0 && (
          <div>
            <span className="text-gray-500 block mb-1">Supported versions</span>
            <div className="flex flex-wrap gap-1">
              {connector.supported_databases.map((db) => (
                <span
                  key={db}
                  className="px-2 py-0.5 bg-gray-100 text-gray-600 rounded text-xs"
                >
                  {db}
                </span>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

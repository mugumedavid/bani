import { useQuery } from '@tanstack/react-query';
import { getConnectors } from '../api/client';

interface ApiConnector {
  name: string;
  class_name: string;
  module: string;
}

const dbIcons: Record<string, string> = {
  postgresql: 'PG',
  mysql: 'My',
  mssql: 'MS',
  oracle: 'Or',
  sqlite: 'SQ',
};

const dbDescriptions: Record<string, string> = {
  postgresql: 'PostgreSQL — open-source relational database',
  mysql: 'MySQL — popular open-source RDBMS',
  mssql: 'Microsoft SQL Server — enterprise relational database',
  oracle: 'Oracle Database — enterprise RDBMS',
  sqlite: 'SQLite — embedded file-based database',
};

export function ConnectorCatalog() {
  const { data: connectors, isLoading, error } = useQuery<ApiConnector[]>({
    queryKey: ['connectors'],
    queryFn: getConnectors as unknown as () => Promise<ApiConnector[]>,
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

function ConnectorCard({ connector }: { connector: ApiConnector }) {
  const abbr = dbIcons[connector.name] ?? connector.name.slice(0, 2).toUpperCase();
  const desc = dbDescriptions[connector.name] ?? connector.module;

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6 hover:shadow-md transition-shadow">
      <div className="flex items-start gap-4 mb-4">
        <div className="w-12 h-12 bg-indigo-100 text-indigo-700 rounded-lg flex items-center justify-center text-lg font-bold flex-shrink-0">
          {abbr}
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <h3 className="font-semibold text-gray-900 truncate">
              {connector.name}
            </h3>
            <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-700">
              source + sink
            </span>
          </div>
          <p className="text-sm text-gray-500 mt-1">{desc}</p>
        </div>
      </div>

      <div className="space-y-2 text-sm">
        <div>
          <span className="text-gray-500 text-xs">Class</span>
          <div className="font-mono text-gray-700 text-xs">{connector.class_name}</div>
        </div>
        <div>
          <span className="text-gray-500 text-xs">Module</span>
          <div className="font-mono text-gray-700 text-xs break-all">{connector.module}</div>
        </div>
      </div>
    </div>
  );
}

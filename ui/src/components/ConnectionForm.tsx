import { useState } from 'react';
import type { ConnectionConfig } from '../types';

interface ConnectionFormProps {
  value: ConnectionConfig;
  onChange: (config: ConnectionConfig) => void;
  connectorOptions: string[];
  label?: string;
}

export function ConnectionForm({
  value,
  onChange,
  connectorOptions,
  label,
}: ConnectionFormProps) {
  const [showExtra, setShowExtra] = useState(false);

  function update(patch: Partial<ConnectionConfig>) {
    onChange({ ...value, ...patch });
  }

  return (
    <div className="space-y-4">
      {label && (
        <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide">
          {label}
        </h3>
      )}

      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">
          Connector
        </label>
        <select
          value={value.connector}
          onChange={(e) => update({ connector: e.target.value })}
          className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
        >
          <option value="">Select a connector...</option>
          {connectorOptions.map((c) => (
            <option key={c} value={c}>
              {c}
            </option>
          ))}
        </select>
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Host
          </label>
          <input
            type="text"
            value={value.host}
            onChange={(e) => update({ host: e.target.value })}
            placeholder="localhost"
            className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
          />
        </div>
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Port
          </label>
          <input
            type="number"
            value={value.port || ''}
            onChange={(e) => update({ port: parseInt(e.target.value) || 0 })}
            placeholder="5432"
            className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
          />
        </div>
      </div>

      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">
          Database
        </label>
        <input
          type="text"
          value={value.database}
          onChange={(e) => update({ database: e.target.value })}
          placeholder="mydb"
          className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
        />
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Username Env Var
          </label>
          <input
            type="text"
            value={value.username_env}
            onChange={(e) => update({ username_env: e.target.value })}
            placeholder="DB_USER"
            className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
          />
          <p className="mt-1 text-xs text-gray-400">
            Environment variable name (not the actual credential)
          </p>
        </div>
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Password Env Var
          </label>
          <input
            type="text"
            value={value.password_env}
            onChange={(e) => update({ password_env: e.target.value })}
            placeholder="DB_PASS"
            className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
          />
          <p className="mt-1 text-xs text-gray-400">
            Environment variable name (not the actual credential)
          </p>
        </div>
      </div>

      <div>
        <button
          type="button"
          onClick={() => setShowExtra(!showExtra)}
          className="text-sm text-indigo-600 hover:text-indigo-800"
        >
          {showExtra ? 'Hide' : 'Show'} extra connection parameters
        </button>
        {showExtra && (
          <div className="mt-2 space-y-2">
            {Object.entries(value.extra).map(([key, val]) => (
              <div key={key} className="flex gap-2 items-center">
                <input
                  type="text"
                  value={key}
                  readOnly
                  className="w-1/3 rounded-md border border-gray-300 px-3 py-1.5 text-sm bg-gray-50"
                />
                <input
                  type="text"
                  value={val}
                  onChange={(e) =>
                    update({
                      extra: { ...value.extra, [key]: e.target.value },
                    })
                  }
                  className="flex-1 rounded-md border border-gray-300 px-3 py-1.5 text-sm"
                />
                <button
                  type="button"
                  onClick={() => {
                    const next = { ...value.extra };
                    delete next[key];
                    update({ extra: next });
                  }}
                  className="text-red-400 hover:text-red-600"
                >
                  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                  </svg>
                </button>
              </div>
            ))}
            <button
              type="button"
              onClick={() => {
                const key = `param_${Object.keys(value.extra).length}`;
                update({ extra: { ...value.extra, [key]: '' } });
              }}
              className="text-sm text-indigo-600 hover:text-indigo-800"
            >
              + Add parameter
            </button>
          </div>
        )}
      </div>
    </div>
  );
}

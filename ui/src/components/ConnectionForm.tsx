import { useState } from 'react';
import type { ConnectionConfig } from '../types';

const CONNECTOR_DEFAULTS: Record<string, Partial<ConnectionConfig>> = {
  postgresql: { port: 5432, host: 'localhost' },
  mysql:      { port: 3306, host: 'localhost' },
  mssql:      { port: 1433, host: 'localhost' },
  oracle:     { port: 1521, host: 'localhost' },
  sqlite:     { port: 0, host: '' },
};

interface ConnectionFormProps {
  value: ConnectionConfig;
  onChange: (config: ConnectionConfig) => void;
  connectorOptions: string[];
  label?: string;
}

function CredentialField({
  label,
  value,
  isEnv,
  onValueChange,
  onModeChange,
  placeholder,
}: {
  label: string;
  value: string;
  isEnv: boolean;
  onValueChange: (v: string) => void;
  onModeChange: (isEnv: boolean) => void;
  placeholder: string;
}) {
  return (
    <div>
      <div className="flex items-center justify-between mb-1">
        <label className="block text-sm font-medium text-gray-700">{label}</label>
        <div className="flex items-center gap-3 text-xs">
          <label className="flex items-center gap-1 cursor-pointer">
            <input
              type="radio"
              checked={!isEnv}
              onChange={() => onModeChange(false)}
              className="w-3 h-3 text-indigo-600"
            />
            <span className={!isEnv ? 'text-gray-700 font-medium' : 'text-gray-400'}>
              Direct
            </span>
          </label>
          <label className="flex items-center gap-1 cursor-pointer">
            <input
              type="radio"
              checked={isEnv}
              onChange={() => onModeChange(true)}
              className="w-3 h-3 text-indigo-600"
            />
            <span className={isEnv ? 'text-gray-700 font-medium' : 'text-gray-400'}>
              Env var
            </span>
          </label>
        </div>
      </div>
      <input
        type={!isEnv && label.toLowerCase().includes('password') ? 'password' : 'text'}
        value={value}
        onChange={(e) => onValueChange(e.target.value)}
        placeholder={isEnv ? 'DB_USER' : placeholder}
        className={`w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 ${
          isEnv ? 'font-mono bg-gray-50' : ''
        }`}
      />
      <p className="mt-0.5 text-xs text-gray-400">
        {isEnv
          ? 'Name of the environment variable (resolved at runtime)'
          : 'Actual credential value'}
      </p>
    </div>
  );
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

  function onConnectorChange(connector: string) {
    const defaults = CONNECTOR_DEFAULTS[connector] ?? {};
    onChange({
      ...value,
      connector,
      port: defaults.port ?? value.port,
      host: defaults.host ?? value.host,
    });
  }

  const isSqlite = value.connector === 'sqlite';

  return (
    <div className="space-y-4">
      {label && (
        <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide">
          {label}
        </h3>
      )}

      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">
          Connection Name
        </label>
        <input
          type="text"
          value={value.name}
          onChange={(e) => update({ name: e.target.value })}
          placeholder="e.g. Production MSSQL, Staging PostgreSQL"
          className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
        />
      </div>

      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">
          Connector
        </label>
        <select
          value={value.connector}
          onChange={(e) => onConnectorChange(e.target.value)}
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

      {!isSqlite && (
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
              className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
            />
          </div>
        </div>
      )}

      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">
          {isSqlite ? 'Database File Path' : 'Database'}
        </label>
        <input
          type="text"
          value={value.database}
          onChange={(e) => update({ database: e.target.value })}
          placeholder={isSqlite ? '/path/to/database.db' : 'mydb'}
          className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
        />
      </div>

      {!isSqlite && (
        <div className="grid grid-cols-2 gap-4">
          <CredentialField
            label="Username"
            value={value.username_env}
            isEnv={value.username_is_env}
            onValueChange={(v) => update({ username_env: v })}
            onModeChange={(isEnv) => update({ username_is_env: isEnv })}
            placeholder="sa"
          />
          <CredentialField
            label="Password"
            value={value.password_env}
            isEnv={value.password_is_env}
            onValueChange={(v) => update({ password_env: v })}
            onModeChange={(isEnv) => update({ password_is_env: isEnv })}
            placeholder="********"
          />
        </div>
      )}

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
                    update({ extra: { ...value.extra, [key]: e.target.value } })
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

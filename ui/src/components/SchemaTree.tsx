import { useState } from 'react';
import type { Table } from '../types';

interface SchemaTreeProps {
  tables: Table[];
}

function TableNode({ table }: { table: Table }) {
  const [expanded, setExpanded] = useState(false);

  return (
    <div className="ml-2">
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex items-center gap-1.5 py-1 px-2 w-full text-left hover:bg-gray-100 rounded text-sm"
      >
        <svg
          className={`w-3 h-3 text-gray-400 transition-transform ${expanded ? 'rotate-90' : ''}`}
          fill="currentColor"
          viewBox="0 0 20 20"
        >
          <path d="M6 4l8 6-8 6V4z" />
        </svg>
        <svg className="w-4 h-4 text-indigo-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 10h18M3 14h18m-9-4v8m-7 0h14a2 2 0 002-2V8a2 2 0 00-2-2H5a2 2 0 00-2 2v8a2 2 0 002 2z" />
        </svg>
        <span className="font-medium text-gray-800">
          {table.schema_name ? `${table.schema_name}.${table.name}` : table.name}
        </span>
        {table.row_count !== null && (
          <span className="text-xs text-gray-400 ml-auto">
            {table.row_count.toLocaleString()} rows
          </span>
        )}
      </button>
      {expanded && (
        <div className="ml-6 border-l border-gray-200 pl-2">
          {table.columns.map((col) => (
            <div
              key={col.name}
              className="flex items-center gap-2 py-0.5 px-2 text-sm text-gray-600"
            >
              {col.is_primary_key && (
                <svg className="w-3 h-3 text-yellow-500 flex-shrink-0" fill="currentColor" viewBox="0 0 20 20">
                  <path d="M10 2a3 3 0 00-3 3c0 1.1.6 2.1 1.5 2.6V9H7a1 1 0 00-1 1v2H5a1 1 0 00-1 1v4a1 1 0 001 1h10a1 1 0 001-1v-4a1 1 0 00-1-1h-1v-2a1 1 0 00-1-1h-1.5V7.6A3 3 0 0010 2zm0 2a1 1 0 110 2 1 1 0 010-2z" />
                </svg>
              )}
              <span className="font-mono text-xs">{col.name}</span>
              <span className="text-xs text-gray-400">{col.data_type}</span>
              {col.nullable && (
                <span className="text-xs text-gray-300">nullable</span>
              )}
            </div>
          ))}
          {table.indexes.length > 0 && (
            <div className="mt-1 pt-1 border-t border-gray-100">
              <span className="text-xs font-medium text-gray-400 px-2">Indexes</span>
              {table.indexes.map((idx) => (
                <div key={idx.name} className="flex items-center gap-2 py-0.5 px-2 text-xs text-gray-500">
                  <span className="font-mono">{idx.name}</span>
                  {idx.is_unique && (
                    <span className="text-xs bg-blue-100 text-blue-700 px-1 rounded">unique</span>
                  )}
                  <span className="text-gray-400">({idx.columns.join(', ')})</span>
                </div>
              ))}
            </div>
          )}
          {table.foreign_keys.length > 0 && (
            <div className="mt-1 pt-1 border-t border-gray-100">
              <span className="text-xs font-medium text-gray-400 px-2">Foreign Keys</span>
              {table.foreign_keys.map((fk) => (
                <div key={fk.name} className="flex items-center gap-2 py-0.5 px-2 text-xs text-gray-500">
                  <span className="font-mono">{fk.name}</span>
                  <span className="text-gray-400">
                    ({fk.columns.join(', ')}) -&gt; {fk.referenced_table}({fk.referenced_columns.join(', ')})
                  </span>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export function SchemaTree({ tables }: SchemaTreeProps) {
  if (tables.length === 0) {
    return (
      <div className="text-center py-8 text-gray-400">
        <p>No tables found.</p>
      </div>
    );
  }

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-3 max-h-[600px] overflow-y-auto">
      <div className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-2 px-2">
        {tables.length} table{tables.length !== 1 ? 's' : ''}
      </div>
      {tables.map((table) => (
        <TableNode
          key={`${table.schema_name ?? ''}.${table.name}`}
          table={table}
        />
      ))}
    </div>
  );
}

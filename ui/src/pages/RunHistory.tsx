import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { getRunHistory, type RunLogEntry } from '../api/client';

const PAGE_SIZE = 15;

const statusColors: Record<string, string> = {
  completed: 'bg-green-100 text-green-700',
  failed: 'bg-red-100 text-red-700',
};

function formatDate(iso: string): string {
  if (!iso) return '-';
  return new Date(iso).toLocaleString();
}

function formatDuration(seconds: number): string {
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  const mins = Math.floor(seconds / 60);
  const secs = seconds % 60;
  return `${mins}m ${secs.toFixed(0)}s`;
}

export function RunHistory() {
  const [page, setPage] = useState(0);

  const { data: runs, isLoading, error } = useQuery<RunLogEntry[]>({
    queryKey: ['runs'],
    queryFn: () => getRunHistory(1000),
  });

  const totalRuns = runs?.length ?? 0;
  const totalPages = Math.max(1, Math.ceil(totalRuns / PAGE_SIZE));
  const pagedRuns = runs?.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE) ?? [];

  return (
    <div>
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-gray-900">Run History</h1>
        <p className="text-gray-500 mt-1">
          Past migration runs
        </p>
      </div>

      {isLoading && (
        <div className="flex items-center justify-center py-16">
          <div className="flex items-center gap-3 text-gray-500">
            <svg className="animate-spin w-5 h-5" fill="none" viewBox="0 0 24 24">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
            </svg>
            Loading run history...
          </div>
        </div>
      )}

      {error && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4">
          <p className="text-sm text-red-700">
            Failed to load run history: {(error as Error).message}
          </p>
        </div>
      )}

      {runs && runs.length === 0 && (
        <div className="text-center py-16 bg-white rounded-xl border border-gray-200">
          <svg className="w-16 h-16 mx-auto text-gray-300 mb-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>
          <h3 className="text-lg font-medium text-gray-900 mb-1">
            No migration runs yet
          </h3>
          <p className="text-gray-500">
            Run a migration to see results here.
          </p>
        </div>
      )}

      {pagedRuns.length > 0 && (
        <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-200 bg-gray-50">
                  <th className="text-left px-6 py-3 font-medium text-gray-500 uppercase tracking-wider text-xs">
                    Project
                  </th>
                  <th className="text-left px-6 py-3 font-medium text-gray-500 uppercase tracking-wider text-xs">
                    Status
                  </th>
                  <th className="text-left px-6 py-3 font-medium text-gray-500 uppercase tracking-wider text-xs">
                    Started
                  </th>
                  <th className="text-left px-6 py-3 font-medium text-gray-500 uppercase tracking-wider text-xs">
                    Duration
                  </th>
                  <th className="text-right px-6 py-3 font-medium text-gray-500 uppercase tracking-wider text-xs">
                    Tables
                  </th>
                  <th className="text-right px-6 py-3 font-medium text-gray-500 uppercase tracking-wider text-xs">
                    Rows
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {pagedRuns.map((run, i) => (
                  <tr key={`${run.project_name}-${run.started_at}-${i}`} className="hover:bg-gray-50">
                    <td className="px-6 py-4">
                      <span className="font-medium text-gray-900">
                        {run.project_name}
                      </span>
                    </td>
                    <td className="px-6 py-4">
                      <span
                        className={`inline-flex px-2.5 py-0.5 rounded-full text-xs font-medium ${
                          statusColors[run.status] ?? 'bg-gray-100 text-gray-700'
                        }`}
                      >
                        {run.status}
                      </span>
                    </td>
                    <td className="px-6 py-4 text-gray-500">
                      {formatDate(run.started_at)}
                    </td>
                    <td className="px-6 py-4 text-gray-500">
                      {formatDuration(run.duration_seconds)}
                    </td>
                    <td className="px-6 py-4 text-right text-gray-500">
                      <span className="text-green-600">{run.tables_completed}</span>
                      {run.tables_failed > 0 && (
                        <span className="text-red-500 ml-1">/ {run.tables_failed} failed</span>
                      )}
                    </td>
                    <td className="px-6 py-4 text-right text-gray-500">
                      {run.total_rows.toLocaleString()}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* Pagination */}
          {totalPages > 1 && (
            <div className="flex items-center justify-between px-6 py-3 border-t border-gray-200 bg-gray-50">
              <p className="text-xs text-gray-500">
                {page * PAGE_SIZE + 1}–{Math.min((page + 1) * PAGE_SIZE, totalRuns)} of {totalRuns} runs
              </p>
              <div className="flex items-center gap-2">
                <button
                  onClick={() => setPage((p) => Math.max(0, p - 1))}
                  disabled={page === 0}
                  className="px-3 py-1 text-xs font-medium rounded border border-gray-300 text-gray-600 hover:bg-white disabled:opacity-30 disabled:cursor-not-allowed"
                >
                  Prev
                </button>
                <span className="text-xs text-gray-500">
                  {page + 1} / {totalPages}
                </span>
                <button
                  onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}
                  disabled={page >= totalPages - 1}
                  className="px-3 py-1 text-xs font-medium rounded border border-gray-300 text-gray-600 hover:bg-white disabled:opacity-30 disabled:cursor-not-allowed"
                >
                  Next
                </button>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

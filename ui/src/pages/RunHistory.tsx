import { useQuery } from '@tanstack/react-query';
import { getMigrationRuns } from '../api/client';
import type { ProjectStatus } from '../types';

const statusColors: Record<ProjectStatus, string> = {
  idle: 'bg-gray-100 text-gray-700',
  running: 'bg-blue-100 text-blue-700',
  completed: 'bg-green-100 text-green-700',
  failed: 'bg-red-100 text-red-700',
};

function formatDuration(seconds: number | null): string {
  if (seconds === null) return '-';
  if (seconds < 60) return `${seconds}s`;
  const mins = Math.floor(seconds / 60);
  const secs = seconds % 60;
  if (mins < 60) return `${mins}m ${secs}s`;
  const hrs = Math.floor(mins / 60);
  const remainMins = mins % 60;
  return `${hrs}h ${remainMins}m`;
}

function formatDate(iso: string): string {
  return new Date(iso).toLocaleString();
}

export function RunHistory() {
  const { data: runs, isLoading, error } = useQuery({
    queryKey: ['runs'],
    queryFn: getMigrationRuns,
  });

  return (
    <div>
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-gray-900">Run History</h1>
        <p className="text-gray-500 mt-1">
          Past migration runs with status and results
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
            Run a migration from the Dashboard to see results here.
          </p>
        </div>
      )}

      {runs && runs.length > 0 && (
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
                  <th className="text-right px-6 py-3 font-medium text-gray-500 uppercase tracking-wider text-xs">
                    Errors
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {runs.map((run) => (
                  <tr key={run.id} className="hover:bg-gray-50">
                    <td className="px-6 py-4">
                      <span className="font-medium text-gray-900">
                        {run.project_name}
                      </span>
                    </td>
                    <td className="px-6 py-4">
                      <span
                        className={`inline-flex px-2.5 py-0.5 rounded-full text-xs font-medium ${statusColors[run.status]}`}
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
                      {run.tables_completed}/{run.tables_total}
                    </td>
                    <td className="px-6 py-4 text-right text-gray-500">
                      {run.rows_transferred.toLocaleString()}
                      {run.rows_total > 0 && (
                        <span className="text-gray-400">
                          /{run.rows_total.toLocaleString()}
                        </span>
                      )}
                    </td>
                    <td className="px-6 py-4 text-right">
                      {run.errors.length > 0 ? (
                        <span className="text-red-600 font-medium">
                          {run.errors.length}
                        </span>
                      ) : (
                        <span className="text-gray-400">0</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}

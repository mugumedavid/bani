import { useState, useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { getProjects } from '../api/client';
import { useWebSocket } from '../hooks/useWebSocket';
import { useAppStore } from '../store/appStore';
import { ProgressBar } from '../components/ProgressBar';

export function MigrationMonitor() {
  const [selectedProjectId, setSelectedProjectId] = useState<string | null>(null);

  const { data: projects } = useQuery({
    queryKey: ['projects'],
    queryFn: getProjects,
    refetchInterval: 5000,
  });

  useWebSocket(selectedProjectId);

  const events = useAppStore((s) => s.migrationEvents);
  const tableProgressMap = useAppStore((s) => s.tableProgress);

  const runningProjects = useMemo(
    () => projects?.filter((p) => p.status === 'running') ?? [],
    [projects],
  );

  const currentProgress = selectedProjectId
    ? tableProgressMap[selectedProjectId]
    : undefined;

  const projectErrors = useMemo(
    () =>
      events
        .filter(
          (e) =>
            e.project_id === selectedProjectId &&
            (e.type === 'table_error' || e.type === 'migration_error'),
        )
        .map((e) => ({
          table: e.table_name,
          error: e.error,
          timestamp: e.timestamp,
        })),
    [events, selectedProjectId],
  );

  const overallStats = useMemo(() => {
    if (!currentProgress) return null;
    const tables = Object.values(currentProgress);
    const totalRows = tables.reduce((a, t) => a + t.total_rows, 0);
    const transferred = tables.reduce((a, t) => a + t.rows_transferred, 0);
    const throughput = tables.reduce((a, t) => a + t.throughput, 0);
    return { totalRows, transferred, throughput, tableCount: tables.length };
  }, [currentProgress]);

  return (
    <div>
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-gray-900">Migration Monitor</h1>
        <p className="text-gray-500 mt-1">
          Real-time view of running migrations
        </p>
      </div>

      {/* Project selector */}
      <div className="bg-white rounded-xl border border-gray-200 p-6 mb-6">
        <label className="block text-sm font-medium text-gray-700 mb-2">
          Select a running migration
        </label>
        {runningProjects.length === 0 ? (
          <p className="text-sm text-gray-400">
            No migrations currently running. Start one from the Dashboard.
          </p>
        ) : (
          <select
            value={selectedProjectId ?? ''}
            onChange={(e) => setSelectedProjectId(e.target.value || null)}
            className="w-full max-w-md rounded-md border border-gray-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
          >
            <option value="">Choose a project...</option>
            {runningProjects.map((p) => (
              <option key={p.id} value={p.id}>
                {p.name}
              </option>
            ))}
          </select>
        )}
      </div>

      {selectedProjectId && (
        <>
          {/* Overall progress */}
          {overallStats && (
            <div className="bg-white rounded-xl border border-gray-200 p-6 mb-6">
              <h2 className="text-lg font-semibold text-gray-900 mb-4">
                Overall Progress
              </h2>
              <ProgressBar
                value={overallStats.transferred}
                max={overallStats.totalRows}
                size="lg"
              />
              <div className="grid grid-cols-3 gap-4 mt-4">
                <StatCard
                  label="Rows Transferred"
                  value={overallStats.transferred.toLocaleString()}
                  sub={`of ${overallStats.totalRows.toLocaleString()}`}
                />
                <StatCard
                  label="Throughput"
                  value={`${overallStats.throughput.toLocaleString()} rows/s`}
                />
                <StatCard
                  label="Tables"
                  value={overallStats.tableCount.toString()}
                />
              </div>
            </div>
          )}

          {/* Per-table progress */}
          {currentProgress && Object.keys(currentProgress).length > 0 && (
            <div className="bg-white rounded-xl border border-gray-200 p-6 mb-6">
              <h2 className="text-lg font-semibold text-gray-900 mb-4">
                Per-Table Progress
              </h2>
              <div className="space-y-4">
                {Object.values(currentProgress).map((tp) => (
                  <div key={tp.table_name}>
                    <div className="flex items-center justify-between mb-1">
                      <span className="text-sm font-medium text-gray-700 font-mono">
                        {tp.table_name}
                      </span>
                      <span className="text-xs text-gray-400">
                        {tp.rows_transferred.toLocaleString()} / {tp.total_rows.toLocaleString()} rows
                        {tp.throughput > 0 && ` | ${tp.throughput.toLocaleString()} rows/s`}
                      </span>
                    </div>
                    <ProgressBar
                      value={tp.rows_transferred}
                      max={tp.total_rows}
                      showPercentage={false}
                      size="sm"
                      color={tp.rows_transferred >= tp.total_rows ? 'green' : 'indigo'}
                    />
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Error log */}
          {projectErrors.length > 0 && (
            <div className="bg-white rounded-xl border border-red-200 p-6">
              <h2 className="text-lg font-semibold text-red-700 mb-4">
                Error Log ({projectErrors.length})
              </h2>
              <div className="space-y-2 max-h-64 overflow-y-auto">
                {projectErrors.map((err, i) => (
                  <div
                    key={i}
                    className="bg-red-50 border border-red-100 rounded p-3 text-sm"
                  >
                    <div className="flex items-center justify-between mb-1">
                      {err.table && (
                        <span className="font-mono font-medium text-red-800">
                          {err.table}
                        </span>
                      )}
                      <span className="text-xs text-red-400">
                        {err.timestamp}
                      </span>
                    </div>
                    <p className="text-red-600">{err.error}</p>
                  </div>
                ))}
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
}

function StatCard({
  label,
  value,
  sub,
}: {
  label: string;
  value: string;
  sub?: string;
}) {
  return (
    <div className="bg-gray-50 rounded-lg p-4">
      <p className="text-xs text-gray-500 uppercase tracking-wide">{label}</p>
      <p className="text-xl font-bold text-gray-900 mt-1">{value}</p>
      {sub && <p className="text-xs text-gray-400">{sub}</p>}
    </div>
  );
}

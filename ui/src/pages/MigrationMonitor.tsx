import { useState, useMemo, useEffect, useRef } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useSearchParams } from 'react-router-dom';
import { getProjects, getMigrateStatus, getCheckpoint, cancelMigration, type MigrateStatusResponse, type ProjectSummary, type CheckpointInfo } from '../api/client';
import { useNavigate } from 'react-router-dom';
import { useSSE } from '../hooks/useSSE';
import { useAppStore, type TableProgressData } from '../store/appStore';
import { ProgressBar } from '../components/ProgressBar';
import { RunMigrationDialog } from '../components/RunMigrationDialog';

export function MigrationMonitor() {
  const [searchParams, setSearchParams] = useSearchParams();
  const justStartedProject = searchParams.get('started');
  const [selectedProjectId, setSelectedProjectId] = useState<string | null>(null);
  const [runDialogProject, setRunDialogProject] = useState<string | null>(null);
  const [waitingForStart, setWaitingForStart] = useState(!!justStartedProject);
  const waitingRef = useRef(waitingForStart);
  const [cancelling, setCancelling] = useState(false);
  const navigate = useNavigate();
  const clearTableProgress = useAppStore((s) => s.clearTableProgress);
  const clearEvents = useAppStore((s) => s.clearMigrationEvents);

  // Clear stale progress from previous run when a new migration is triggered
  useEffect(() => {
    if (justStartedProject) {
      clearTableProgress();
      clearEvents();
    }
  }, [justStartedProject, clearTableProgress, clearEvents]);

  const { data: projects } = useQuery<ProjectSummary[]>({
    queryKey: ['projects'],
    queryFn: getProjects,
  });

  const { data: migrateStatus } = useQuery<MigrateStatusResponse>({
    queryKey: ['migrateStatus'],
    queryFn: getMigrateStatus,
    refetchInterval: (query) => {
      const running = query.state.data?.running;
      if (running) return 2000;
      if (waitingRef.current) return 500;
      return false;
    },
  });

  const isRunning = migrateStatus?.running ?? false;
  const phase = migrateStatus?.phase;

  // Connect SSE stream while migration is running
  useSSE(isRunning);

  const elapsed = migrateStatus?.elapsed_seconds ?? 0;

  // Check for existing checkpoint when a project is selected
  const { data: checkpointInfo } = useQuery<CheckpointInfo>({
    queryKey: ['checkpoint', selectedProjectId],
    queryFn: () => getCheckpoint(selectedProjectId!),
    enabled: !!selectedProjectId && !isRunning,
  });

  // Redirect to dashboard when cancellation completes
  useEffect(() => {
    if (cancelling && !isRunning) {
      setCancelling(false);
      navigate('/');
    }
  }, [cancelling, isRunning, navigate]);

  // When we detect running=true after a "just started" navigation, stop fast polling
  useEffect(() => {
    if (isRunning && waitingForStart) {
      setWaitingForStart(false);
      waitingRef.current = false;
      setSearchParams({}, { replace: true });
    }
  }, [isRunning, waitingForStart, setSearchParams]);

  // Timeout: stop waiting after 15 seconds if migration never starts
  useEffect(() => {
    if (!waitingForStart) return;
    const timer = setTimeout(() => {
      setWaitingForStart(false);
      waitingRef.current = false;
    }, 15000);
    return () => clearTimeout(timer);
  }, [waitingForStart]);

  const events = useAppStore((s) => s.migrationEvents);
  const tableProgress = useAppStore((s) => s.tableProgress);

  // Show only tables that have started (running/completed/failed).
  // Running tables at top, completed accumulate below.
  const activeTables = useMemo(() => {
    const tables = Object.values(tableProgress).filter(
      (t) => t.status !== 'pending',
    );
    const order: Record<string, number> = { running: 0, completed: 1, failed: 2 };
    return tables.sort((a, b) => (order[a.status] ?? 3) - (order[b.status] ?? 3));
  }, [tableProgress]);

  const totalTableCount = Object.keys(tableProgress).length;
  const completedCount = activeTables.filter(t => t.status === 'completed').length;
  const hasTableProgress = activeTables.length > 0;

  // SSE errors
  const sseErrors = useMemo(
    () =>
      events
        .filter((e) => e.type === 'table_create_failed')
        .map((e) => ({
          table: e.table_name ?? 'unknown',
          error: e.reason ?? 'Unknown error',
        })),
    [events],
  );

  const tablePct = totalTableCount > 0
    ? Math.round((completedCount / totalTableCount) * 100)
    : 0;

  return (
    <div>
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-gray-900">Migration Monitor</h1>
        <p className="text-gray-500 mt-1">
          Real-time view of running migrations
        </p>
      </div>

      {/* "Starting..." banner while waiting for background thread */}
      {waitingForStart && !isRunning && (
        <div className="bg-indigo-50 border border-indigo-200 rounded-xl p-6 mb-6">
          <div className="flex items-center gap-3">
            <Spinner />
            <div>
              <p className="text-sm font-semibold text-indigo-900">
                Starting migration{justStartedProject ? ` — ${justStartedProject}` : ''}...
              </p>
              <p className="text-xs text-indigo-600 mt-0.5">
                Waiting for the migration to begin
              </p>
            </div>
          </div>
        </div>
      )}

      {/* Start Migration section — shown when idle */}
      {!isRunning && !waitingForStart && (
        <div className="bg-white rounded-xl border border-gray-200 p-6 mb-6">
          <h2 className="text-sm font-semibold text-gray-900 mb-3">
            Start a Migration
          </h2>
          <div className="flex items-end gap-3">
            <div className="flex-1 max-w-md">
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Project
              </label>
              <select
                value={selectedProjectId ?? ''}
                onChange={(e) => setSelectedProjectId(e.target.value || null)}
                className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
              >
                <option value="">Choose a project...</option>
                {(projects ?? []).map((p) => (
                  <option key={p.name} value={p.name}>
                    {p.name}
                  </option>
                ))}
              </select>
            </div>
            <button
              onClick={() => {
                if (selectedProjectId) {
                  setRunDialogProject(selectedProjectId);
                }
              }}
              disabled={!selectedProjectId}
              className="inline-flex items-center gap-2 px-5 py-2 bg-indigo-600 text-white text-sm font-medium rounded-lg hover:bg-indigo-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
            >
              Run
            </button>
          </div>
          {/* Checkpoint notice */}
          {checkpointInfo?.exists && (
            <div className="mt-3 bg-amber-50 border border-amber-200 rounded-lg px-4 py-3 flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-amber-900">
                  Checkpoint available
                </p>
                <p className="text-xs text-amber-700 mt-0.5">
                  {checkpointInfo.tables_completed} / {checkpointInfo.tables_total} tables
                  completed from a previous run
                </p>
              </div>
              <span className="text-xs text-amber-600">
                Use &ldquo;Resume from checkpoint&rdquo; when starting to continue
              </span>
            </div>
          )}
        </div>
      )}

      {/* Introspection phase */}
      {isRunning && phase === 'introspecting' && (
        <div className="bg-indigo-50 border border-indigo-200 rounded-xl p-6 mb-6">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <Spinner />
              <div>
                <h2 className="text-lg font-semibold text-indigo-900">
                  Analysing source schema...
                </h2>
                {migrateStatus?.project_name && (
                  <p className="text-sm text-indigo-700">
                    Project: {migrateStatus.project_name}
                  </p>
                )}
                <p className="text-xs text-indigo-600 mt-0.5">
                  Discovering tables, columns, and row counts
                </p>
              </div>
            </div>
            <ElapsedTime seconds={elapsed} />
          </div>
        </div>
      )}

      {/* Active migration — transferring phase */}
      {isRunning && phase === 'transferring' && migrateStatus && (
        <div className="bg-indigo-50 border border-indigo-200 rounded-xl p-6 mb-6">
          <div className="flex items-center justify-between mb-3">
            <div className="flex items-center gap-3">
              <Spinner />
              <div>
                <h2 className="text-lg font-semibold text-indigo-900">
                  {cancelling ? 'Cancellation requested...' : 'Transferring data'}
                </h2>
                {migrateStatus.project_name && (
                  <p className="text-sm text-indigo-700">
                    Project: {migrateStatus.project_name}
                  </p>
                )}
              </div>
            </div>
            <div className="flex items-center gap-3">
              <ElapsedTime seconds={elapsed} />
              {!cancelling && (
                <button
                  onClick={async () => {
                    setCancelling(true);
                    try { await cancelMigration(); } catch { /* ignore */ }
                  }}
                  className="px-3 py-1 text-xs font-medium text-red-600 border border-red-300 rounded-lg hover:bg-red-50 cursor-pointer transition-colors"
                >
                  Cancel
                </button>
              )}
            </div>
          </div>

          {/* Current table */}
          {migrateStatus.current_table && (
            <div className="bg-white/70 rounded-lg px-4 py-3 mb-3 flex items-center gap-2">
              <span className="relative flex h-2.5 w-2.5">
                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-indigo-400 opacity-75" />
                <span className="relative inline-flex rounded-full h-2.5 w-2.5 bg-indigo-500" />
              </span>
              <span className="text-sm text-indigo-900">
                Currently migrating:{' '}
                <span className="font-mono font-semibold">{migrateStatus.current_table}</span>
              </span>
            </div>
          )}

          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <MiniStatWithBar
              label="Tables Completed"
              value={`${migrateStatus.tables_completed} / ${migrateStatus.total_tables}`}
              pct={tablePct}
            />
            <MiniStat label="Tables Failed" value={String(migrateStatus.tables_failed)} />
            <MiniStat label="Rows Read" value={migrateStatus.total_rows_read.toLocaleString()} />
            <MiniStat label="Rows Written" value={migrateStatus.total_rows_written.toLocaleString()} />
          </div>

          {migrateStatus.table_failures && migrateStatus.table_failures.length > 0 && (
            <FailureList failures={migrateStatus.table_failures} />
          )}

          {migrateStatus.warnings && migrateStatus.warnings.length > 0 && (
            <WarningsList warnings={migrateStatus.warnings} />
          )}

          {migrateStatus.error && (
            <div className="mt-3 bg-red-50 border border-red-200 rounded-lg p-3">
              <p className="text-sm text-red-700">{migrateStatus.error}</p>
            </div>
          )}
        </div>
      )}

      {/* Index/FK creation phase */}
      {isRunning && phase === 'indexes' && migrateStatus && (
        <div className="bg-indigo-50 border border-indigo-200 rounded-xl p-6 mb-6">
          <div className="flex items-center justify-between mb-3">
            <div className="flex items-center gap-3">
              <Spinner />
              <div>
                <h2 className="text-lg font-semibold text-indigo-900">
                  Creating indexes and foreign keys...
                </h2>
                {migrateStatus.project_name && (
                  <p className="text-sm text-indigo-700">
                    Project: {migrateStatus.project_name}
                  </p>
                )}
                <p className="text-xs text-indigo-600 mt-0.5">
                  Data transfer complete — finalising schema constraints
                </p>
              </div>
            </div>
            <ElapsedTime seconds={elapsed} />
          </div>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <MiniStatWithBar
              label="Tables Completed"
              value={`${migrateStatus.tables_completed} / ${migrateStatus.total_tables}`}
              pct={tablePct}
            />
            <MiniStat label="Tables Failed" value={String(migrateStatus.tables_failed)} />
            <MiniStat label="Rows Read" value={migrateStatus.total_rows_read.toLocaleString()} />
            <MiniStat label="Rows Written" value={migrateStatus.total_rows_written.toLocaleString()} />
          </div>
        </div>
      )}

      {/* Hook execution phase */}
      {isRunning && phase?.startsWith('hooks:') && migrateStatus && (
        <div className="bg-indigo-50 border border-indigo-200 rounded-xl p-6 mb-6">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <Spinner />
              <div>
                <h2 className="text-lg font-semibold text-indigo-900">
                  Running {phase.replace('hooks:', '')} hooks...
                </h2>
                {migrateStatus.project_name && (
                  <p className="text-sm text-indigo-700">
                    Project: {migrateStatus.project_name}
                  </p>
                )}
              </div>
            </div>
            <ElapsedTime seconds={elapsed} />
          </div>
        </div>
      )}

      {/* Completed/idle status */}
      {!isRunning && !waitingForStart && migrateStatus && migrateStatus.project_name && (
        <div className={`rounded-xl border p-6 mb-6 ${migrateStatus.error ? 'bg-red-50 border-red-200' : 'bg-green-50 border-green-200'}`}>
          <div className="flex items-center justify-between">
            <div>
              <h2 className={`text-lg font-semibold ${migrateStatus.error ? 'text-red-900' : 'text-green-900'}`}>
                Migration {migrateStatus.error ? 'Failed' : 'Completed'}
              </h2>
              <p className={`text-sm mt-1 ${migrateStatus.error ? 'text-red-700' : 'text-green-700'}`}>
                Project: {migrateStatus.project_name}
              </p>
            </div>
            {migrateStatus.elapsed_seconds > 0 && (
              <ElapsedTime seconds={migrateStatus.elapsed_seconds} />
            )}
          </div>
          {migrateStatus.error && (
            <p className="text-sm text-red-600 mt-2">{migrateStatus.error}</p>
          )}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mt-3">
            <MiniStat label="Tables Completed" value={String(migrateStatus.tables_completed)} />
            <MiniStat label="Tables Failed" value={String(migrateStatus.tables_failed)} />
            <MiniStat label="Rows Read" value={migrateStatus.total_rows_read.toLocaleString()} />
            <MiniStat label="Rows Written" value={migrateStatus.total_rows_written.toLocaleString()} />
          </div>
          {migrateStatus.table_failures && migrateStatus.table_failures.length > 0 && (
            <FailureList failures={migrateStatus.table_failures} />
          )}
          {migrateStatus.warnings && migrateStatus.warnings.length > 0 && (
            <WarningsList warnings={migrateStatus.warnings} />
          )}
        </div>
      )}

      {/* Per-table progress bars */}
      {hasTableProgress && (
        <div className="bg-white rounded-xl border border-gray-200 p-6 mb-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">
            Table Progress ({completedCount} / {totalTableCount})
          </h2>
          <div className="space-y-2 max-h-[32rem] overflow-y-auto">
            {activeTables.map((tp) => (
              <TableRow key={tp.table_name} table={tp} />
            ))}
          </div>
        </div>
      )}

      {/* SSE error log */}
      {sseErrors.length > 0 && (
        <div className="bg-white rounded-xl border border-red-200 p-6">
          <h2 className="text-lg font-semibold text-red-700 mb-4">
            Error Log ({sseErrors.length})
          </h2>
          <div className="space-y-2 max-h-64 overflow-y-auto">
            {sseErrors.map((err, i) => (
              <div
                key={i}
                className="bg-red-50 border border-red-100 rounded p-3 text-sm"
              >
                <span className="font-mono font-medium text-red-800">
                  {err.table}
                </span>
                <p className="text-red-600 mt-1">{err.error}</p>
              </div>
            ))}
          </div>
        </div>
      )}

      <RunMigrationDialog
        projectName={runDialogProject ?? ''}
        open={runDialogProject !== null}
        onClose={() => setRunDialogProject(null)}
      />
    </div>
  );
}

/* --- Sub-components --- */

function TableRow({ table }: { table: TableProgressData }) {
  // For completed tables with 0 or unknown total, show full bar
  const isComplete = table.status === 'completed';
  const effectiveMax = table.total_rows > 0 ? table.total_rows : (isComplete ? 1 : 1);
  const effectiveVal = isComplete && table.total_rows === 0 ? 1 : table.rows_transferred;

  const pct =
    table.total_rows > 0
      ? Math.min(100, Math.round((table.rows_transferred / table.total_rows) * 100))
      : isComplete ? 100 : 0;

  const statusColor: Record<string, string> = {
    pending: 'text-gray-400',
    running: 'text-green-600',
    completed: 'text-green-600',
    failed: 'text-red-600',
  };

  const barColor: Record<string, 'indigo' | 'green' | 'red' | 'yellow'> = {
    pending: 'yellow',
    running: 'indigo',
    completed: 'green',
    failed: 'red',
  };

  const shortName = table.table_name.split('.').pop() ?? table.table_name;

  return (
    <div className="flex items-center gap-3 py-1">
      <span
        className={`text-xs font-mono w-48 truncate flex-shrink-0 ${statusColor[table.status]}`}
        title={table.table_name}
      >
        {shortName}
      </span>
      <div className="flex-1 min-w-0">
        <ProgressBar
          value={effectiveVal}
          max={effectiveMax}
          showPercentage={false}
          size="sm"
          color={barColor[table.status]}
        />
      </div>
      <span className="text-xs text-gray-500 w-20 text-right flex-shrink-0">
        {table.status === 'failed'
          ? 'failed'
          : table.total_rows > 0
            ? `${pct}%`
            : isComplete
              ? 'done'
              : '—'}
      </span>
    </div>
  );
}

function MiniStat({ label, value }: { label: string; value: string }) {
  return (
    <div className="bg-white/60 rounded-lg px-3 py-2">
      <p className="text-xs text-gray-500">{label}</p>
      <p className="text-sm font-semibold text-gray-900">{value}</p>
    </div>
  );
}

function MiniStatWithBar({ label, value, pct }: { label: string; value: string; pct: number }) {
  return (
    <div className="bg-white/60 rounded-lg px-3 py-2">
      <p className="text-xs text-gray-500">{label}</p>
      <p className="text-sm font-semibold text-gray-900">{value}</p>
      <div className="mt-1 h-1.5 bg-gray-200 rounded-full overflow-hidden">
        <div
          className="h-full bg-green-500 rounded-full transition-all duration-300"
          style={{ width: `${pct}%` }}
        />
      </div>
    </div>
  );
}

function ElapsedTime({ seconds }: { seconds: number }) {
  const mins = Math.floor(seconds / 60);
  const secs = seconds % 60;
  const display = mins > 0
    ? `${mins}m ${secs.toString().padStart(2, '0')}s`
    : `${secs}s`;

  return (
    <span className="text-sm font-mono text-indigo-400 flex-shrink-0">
      {display}
    </span>
  );
}

function FailureList({ failures }: { failures: string[] }) {
  return (
    <div className="mt-3 bg-amber-50 border border-amber-200 rounded-lg p-3">
      <p className="text-xs font-semibold text-amber-800 mb-1">
        Table Failures ({failures.length})
      </p>
      <ul className="text-xs text-amber-700 space-y-0.5 max-h-32 overflow-y-auto">
        {failures.map((f, i) => (
          <li key={i} className="truncate">{f}</li>
        ))}
      </ul>
    </div>
  );
}

function WarningsList({ warnings }: { warnings: string[] }) {
  return (
    <div className="mt-3 bg-blue-50 border border-blue-200 rounded-lg p-3">
      <p className="text-xs font-semibold text-blue-800 mb-1">
        Warnings ({warnings.length})
      </p>
      <ul className="text-xs text-blue-700 space-y-0.5 max-h-48 overflow-y-auto">
        {warnings.map((w, i) => (
          <li key={i} className="font-mono">{w}</li>
        ))}
      </ul>
    </div>
  );
}

function Spinner() {
  return (
    <svg className="animate-spin w-5 h-5 text-indigo-600 flex-shrink-0" fill="none" viewBox="0 0 24 24">
      <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
      <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
    </svg>
  );
}

import { useState } from 'react';
import { useQuery, useQueries, useMutation, useQueryClient } from '@tanstack/react-query';
import { Link } from 'react-router-dom';
import {
  getProjects,
  deleteProject,
  getRunSummary,
  getRunHistory,
  getMigrateStatus,
  getCheckpoint,
  deleteCheckpoint,
  getSchedules,
  getLastRunPerProject,
  type RunLogEntry,
  type RunSummary,
  type MigrateStatusResponse,
  type ProjectSummary,
  type CheckpointInfo,
  type ScheduleInfo,
} from '../api/client';
import { RunMigrationDialog } from '../components/RunMigrationDialog';

function formatTimeAgo(iso: string): string {
  if (!iso) return '-';
  const now = Date.now();
  const then = new Date(iso).getTime();
  const diff = now - then;
  if (diff < 0) return 'just now';
  const seconds = Math.floor(diff / 1000);
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  if (days === 1) return 'yesterday';
  if (days < 30) return `${days}d ago`;
  return new Date(iso).toLocaleDateString();
}

function formatNumber(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return n.toLocaleString();
}

/* --- Stat Card --- */
function StatCard({
  icon,
  value,
  label,
  accent,
}: {
  icon: React.ReactNode;
  value: string;
  label: string;
  accent?: string;
}) {
  return (
    <div className="bg-white rounded-lg border border-gray-200 px-4 py-3 flex items-center gap-3">
      <div
        className={`flex-shrink-0 w-9 h-9 rounded-lg flex items-center justify-center ${accent ?? 'bg-gray-100 text-gray-500'}`}
      >
        {icon}
      </div>
      <div className="min-w-0">
        <p className="text-lg font-semibold text-gray-900 leading-tight truncate">
          {value}
        </p>
        <p className="text-xs text-gray-500 truncate">{label}</p>
      </div>
    </div>
  );
}

/* --- Active Migration Banner --- */
function ActiveMigration({ status }: { status: MigrateStatusResponse }) {
  if (!status.running) return null;
  const total = status.tables_completed + status.tables_failed;
  return (
    <Link
      to="/monitor"
      className="block bg-indigo-50 border border-indigo-200 rounded-lg px-5 py-3 mb-6 hover:bg-indigo-100 transition-colors"
    >
      <div className="flex items-center gap-3">
        <div className="flex-shrink-0">
          <svg
            className="animate-spin w-5 h-5 text-indigo-600"
            fill="none"
            viewBox="0 0 24 24"
          >
            <circle
              className="opacity-25"
              cx="12"
              cy="12"
              r="10"
              stroke="currentColor"
              strokeWidth="4"
            />
            <path
              className="opacity-75"
              fill="currentColor"
              d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"
            />
          </svg>
        </div>
        <div className="flex-1 min-w-0">
          <p className="text-sm font-semibold text-indigo-900">
            Migration running{' '}
            {status.project_name && (
              <span className="font-normal text-indigo-700">
                &mdash; {status.project_name}
              </span>
            )}
          </p>
          <p className="text-xs text-indigo-600 mt-0.5">
            {total} table{total !== 1 ? 's' : ''} processed
            {' / '}
            {status.total_rows_written.toLocaleString()} rows written
            {status.current_table && (
              <span className="ml-2 text-indigo-400">
                &middot; {status.current_table}
              </span>
            )}
          </p>
        </div>
        <div className="flex items-center gap-3 flex-shrink-0">
          {status.elapsed_seconds > 0 && (
            <span className="text-xs font-mono text-indigo-400">
              {status.elapsed_seconds >= 60
                ? `${Math.floor(status.elapsed_seconds / 60)}m ${(status.elapsed_seconds % 60).toString().padStart(2, '0')}s`
                : `${status.elapsed_seconds}s`}
            </span>
          )}
          <span className="text-xs text-indigo-500 font-medium">
            View monitor &rarr;
          </span>
        </div>
      </div>
    </Link>
  );
}

/* --- Recent Runs List --- */
function RecentRuns({ runs }: { runs: RunLogEntry[] }) {
  if (!runs || runs.length === 0) {
    return (
      <p className="text-sm text-gray-400 py-4 text-center">
        No runs yet
      </p>
    );
  }
  return (
    <ul className="divide-y divide-gray-100">
      {runs.slice(0, 5).map((run, i) => (
        <li key={`${run.project_name}-${run.started_at}-${i}`} className="flex items-center gap-2 py-2.5">
          <span
            className={`flex-shrink-0 w-2 h-2 rounded-full ${run.status === 'completed' ? 'bg-green-500' : 'bg-red-500'}`}
          />
          <span className="text-sm font-medium text-gray-900 truncate flex-1">
            {run.project_name}
          </span>
          <span className="text-xs text-gray-400 flex-shrink-0">
            {formatTimeAgo(run.finished_at)}
          </span>
          <span className="text-xs text-gray-400 flex-shrink-0">
            {formatNumber(run.total_rows)} rows
          </span>
        </li>
      ))}
    </ul>
  );
}

/* --- Main Dashboard --- */
export function Dashboard() {
  const queryClient = useQueryClient();
  const [runDialogProject, setRunDialogProject] = useState<string | null>(null);

  const { data: projects, isLoading: projectsLoading, error: projectsError } = useQuery<ProjectSummary[]>({
    queryKey: ['projects'],
    queryFn: getProjects,
  });

  const { data: runSummary } = useQuery<RunSummary>({
    queryKey: ['runSummary'],
    queryFn: getRunSummary,
  });

  const { data: recentRuns } = useQuery<RunLogEntry[]>({
    queryKey: ['recentRuns'],
    queryFn: () => getRunHistory(5),
  });

  const { data: migrateStatus } = useQuery<MigrateStatusResponse>({
    queryKey: ['migrateStatus'],
    queryFn: getMigrateStatus,
    refetchInterval: (query) =>
      query.state.data?.running ? 5000 : false,
  });

  const deleteMutation = useMutation({
    mutationFn: deleteProject,
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['projects'] }),
  });

  // Fetch checkpoint info for all projects in parallel
  const checkpointQueries = useQueries({
    queries: (projects ?? []).map((p) => ({
      queryKey: ['checkpoint', p.name],
      queryFn: () => getCheckpoint(p.name),
      staleTime: 30_000,
    })),
  });
  const checkpointMap: Record<string, CheckpointInfo> = {};
  (projects ?? []).forEach((p, i) => {
    if (checkpointQueries[i]?.data) {
      checkpointMap[p.name] = checkpointQueries[i].data;
    }
  });

  const deleteCheckpointMutation = useMutation({
    mutationFn: deleteCheckpoint,
    onSuccess: (_data, projectName) => {
      queryClient.invalidateQueries({ queryKey: ['checkpoint', projectName] });
    },
  });

  // Fetch schedule info for all projects
  const { data: schedules } = useQuery<ScheduleInfo[]>({
    queryKey: ['schedules'],
    queryFn: getSchedules,
    staleTime: 30_000,
  });
  const scheduleMap: Record<string, ScheduleInfo> = {};
  for (const s of schedules ?? []) {
    scheduleMap[s.project] = s;
  }

  // Fetch last run status per project
  const { data: lastRuns } = useQuery<Record<string, RunLogEntry>>({
    queryKey: ['lastRunPerProject'],
    queryFn: getLastRunPerProject,
    staleTime: 30_000,
  });

  if (projectsLoading) {
    return (
      <div className="flex items-center justify-center py-20">
        <svg
          className="animate-spin w-8 h-8 text-indigo-500"
          fill="none"
          viewBox="0 0 24 24"
        >
          <circle
            className="opacity-25"
            cx="12"
            cy="12"
            r="10"
            stroke="currentColor"
            strokeWidth="4"
          />
          <path
            className="opacity-75"
            fill="currentColor"
            d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"
          />
        </svg>
      </div>
    );
  }

  if (projectsError) {
    return (
      <div className="bg-red-50 border border-red-200 rounded-lg p-4">
        <p className="text-sm text-red-700">
          Failed to load projects: {(projectsError as Error).message}
        </p>
      </div>
    );
  }

  const lastRun = runSummary?.last_run;
  const lastRunStatus = lastRun?.status ?? '-';
  const lastRunTime = lastRun?.finished_at
    ? formatTimeAgo(lastRun.finished_at)
    : '-';

  return (
    <div>
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Dashboard</h1>
          <p className="text-gray-500 mt-1">
            Manage your migration projects
          </p>
        </div>
      </div>

      {/* Status Summary Cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 mb-6">
        <StatCard
          icon={
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 7v10a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-6l-2-2H5a2 2 0 00-2 2z" />
            </svg>
          }
          value={String(projects?.length ?? 0)}
          label="Total Projects"
          accent="bg-indigo-100 text-indigo-600"
        />
        <StatCard
          icon={
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
            </svg>
          }
          value={String(runSummary?.total_runs ?? 0)}
          label="Total Runs"
          accent="bg-blue-100 text-blue-600"
        />
        <StatCard
          icon={
            lastRunStatus === 'completed' ? (
              <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
              </svg>
            ) : lastRunStatus === 'failed' ? (
              <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              </svg>
            ) : (
              <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
            )
          }
          value={lastRunTime}
          label={`Last Run${lastRunStatus !== '-' ? ` (${lastRunStatus})` : ''}`}
          accent={
            lastRunStatus === 'completed'
              ? 'bg-green-100 text-green-600'
              : lastRunStatus === 'failed'
                ? 'bg-red-100 text-red-600'
                : 'bg-gray-100 text-gray-500'
          }
        />
        <StatCard
          icon={
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4" />
            </svg>
          }
          value={formatNumber(runSummary?.lifetime_rows ?? 0)}
          label="Lifetime Rows"
          accent="bg-purple-100 text-purple-600"
        />
      </div>

      {/* Active Migration */}
      {migrateStatus && <ActiveMigration status={migrateStatus} />}

      {/* Recent Runs + Quick Actions */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 mb-6">
        {/* Recent Runs */}
        <div className="lg:col-span-2 bg-white rounded-lg border border-gray-200 px-5 py-4">
          <div className="flex items-center justify-between mb-2">
            <h2 className="text-sm font-semibold text-gray-900">
              Recent Runs
            </h2>
            <Link
              to="/history"
              className="text-xs text-indigo-600 hover:text-indigo-800 font-medium"
            >
              View all &rarr;
            </Link>
          </div>
          <RecentRuns runs={recentRuns ?? []} />
        </div>

        {/* Quick Actions */}
        <div className="bg-white rounded-lg border border-gray-200 px-5 py-4">
          <h2 className="text-sm font-semibold text-gray-900 mb-3">
            Quick Actions
          </h2>
          <div className="space-y-2">
            <Link
              to="/projects/new"
              className="flex items-center gap-2 w-full px-3 py-2.5 bg-indigo-600 text-white text-sm font-medium rounded-lg hover:bg-indigo-700 transition-colors justify-center"
            >
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
              </svg>
              New Project
            </Link>
            <Link
              to="/schema"
              className="flex items-center gap-2 w-full px-3 py-2.5 bg-white border border-gray-200 text-gray-700 text-sm font-medium rounded-lg hover:bg-gray-50 transition-colors justify-center"
            >
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4" />
              </svg>
              Browse Schema
            </Link>
          </div>
        </div>
      </div>

      {/* Projects */}
      <div>
        <h2 className="text-sm font-semibold text-gray-900 mb-3">
          Projects
        </h2>
        {!projects || projects.length === 0 ? (
          <div className="bg-white rounded-xl border border-gray-200 p-12 text-center">
            <svg
              className="w-16 h-16 mx-auto mb-4 text-gray-300"
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={1}
                d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"
              />
            </svg>
            <h3 className="text-lg font-medium text-gray-900 mb-1">
              No projects yet
            </h3>
            <p className="text-gray-500 mb-4">
              Create your first migration project to get started.
            </p>
            <Link
              to="/projects/new"
              className="inline-flex items-center gap-2 px-4 py-2 bg-indigo-600 text-white text-sm rounded-lg hover:bg-indigo-700"
            >
              Create Project
            </Link>
          </div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-3 max-h-[28rem] overflow-y-auto">
            {projects.map((project) => {
              const ckpt = checkpointMap[project.name];
              const hasCheckpoint = ckpt?.exists;
              const sched = scheduleMap[project.name];
              const lastRun = lastRuns?.[project.name];
              const isProjectRunning = migrateStatus?.running && migrateStatus.project_name === project.name;
              return (
                <div
                  key={project.name}
                  className="bg-white rounded-lg border border-gray-200 px-4 py-3 hover:shadow-sm transition-shadow"
                >
                  <div className="flex items-center justify-between mb-1">
                    <h3 className="text-sm font-semibold text-gray-900 truncate">
                      {project.name}
                    </h3>
                    <div className="flex items-center gap-1.5 ml-2 flex-shrink-0">
                      {/* Last run status indicator */}
                      {lastRun && !isProjectRunning && (
                        <span title={lastRun.status === 'completed' ? 'Last run succeeded' : 'Last run failed'}>
                          {lastRun.status === 'completed' ? (
                            <svg className="w-3.5 h-3.5 text-green-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2.5} d="M5 13l4 4L19 7" />
                            </svg>
                          ) : (
                            <svg className="w-3.5 h-3.5 text-red-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2.5} d="M6 18L18 6M6 6l12 12" />
                            </svg>
                          )}
                        </span>
                      )}
                      {/* Schedule clock icon */}
                      {sched && (
                        <span title={sched.status === 'active' ? `Scheduled: ${sched.cron}` : 'Schedule failed to start'}>
                          <svg className={`w-3.5 h-3.5 ${sched.status === 'active' ? 'text-blue-500' : 'text-red-400'}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
                          </svg>
                        </span>
                      )}
                      {/* Running indicator */}
                      {isProjectRunning && (
                        <svg className="animate-spin w-3.5 h-3.5 text-indigo-500" fill="none" viewBox="0 0 24 24">
                          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                        </svg>
                      )}
                      {/* Checkpoint badge — only when NOT running */}
                      {hasCheckpoint && !isProjectRunning && (
                        <span className="px-1.5 py-0.5 rounded text-[10px] font-medium bg-amber-100 text-amber-700">
                          checkpoint
                        </span>
                      )}
                    </div>
                  </div>
                  {sched?.next_run && (
                    <p className="text-[11px] text-blue-500 mb-1">
                      Next run: {new Date(sched.next_run).toLocaleString()}
                    </p>
                  )}
                  {/* Checkpoint info — only when NOT running */}
                  {hasCheckpoint && !isProjectRunning && (
                    <div className="mb-1.5 flex items-center justify-between">
                      <p className="text-[11px] text-amber-600">
                        {ckpt.tables_completed} / {ckpt.tables_total} tables completed
                      </p>
                      <div className="flex items-center gap-2">
                        <button
                          onClick={() => setRunDialogProject(project.name)}
                          className="text-[10px] text-amber-700 hover:text-indigo-600 font-medium cursor-pointer"
                        >
                          Resume
                        </button>
                        <button
                          onClick={() => {
                            if (confirm(`Delete checkpoint for "${project.name}"?`)) {
                              deleteCheckpointMutation.mutate(project.name);
                            }
                          }}
                          className="text-[10px] text-amber-500 hover:text-red-600 font-medium cursor-pointer"
                        >
                          Clear
                        </button>
                      </div>
                    </div>
                  )}
                  <div className="flex items-center gap-2">
                    <button
                      onClick={() => setRunDialogProject(project.name)}
                      className="text-xs text-indigo-600 hover:text-green-600 font-medium cursor-pointer"
                    >
                      Run
                    </button>
                    <Link
                      to={`/projects/${project.name}`}
                      className="text-xs text-gray-500 hover:text-gray-700 font-medium"
                    >
                      Edit
                    </Link>
                    <span className="text-gray-300">|</span>
                    <button
                      onClick={() => {
                        if (confirm(`Delete "${project.name}"?`)) {
                          deleteMutation.mutate(project.name);
                        }
                      }}
                      className="text-xs text-gray-400 hover:text-red-500 font-medium cursor-pointer"
                    >
                      Delete
                    </button>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* Run Migration Dialog */}
      <RunMigrationDialog
        projectName={runDialogProject ?? ''}
        open={runDialogProject !== null}
        onClose={() => setRunDialogProject(null)}
      />
    </div>
  );
}

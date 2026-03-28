import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Link } from 'react-router-dom';
import { getProjects, deleteProject, startMigration } from '../api/client';
import type { Project, ProjectStatus } from '../types';

const statusColors: Record<ProjectStatus, string> = {
  idle: 'bg-gray-100 text-gray-700',
  running: 'bg-blue-100 text-blue-700',
  completed: 'bg-green-100 text-green-700',
  failed: 'bg-red-100 text-red-700',
};

export function Dashboard() {
  const queryClient = useQueryClient();

  const { data: projects, isLoading, error } = useQuery({
    queryKey: ['projects'],
    queryFn: getProjects,
  });

  const deleteMutation = useMutation({
    mutationFn: deleteProject,
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['projects'] }),
  });

  const runMutation = useMutation({
    mutationFn: startMigration,
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['projects'] }),
  });

  if (isLoading) {
    return <LoadingState />;
  }

  if (error) {
    return (
      <ErrorAlert message={`Failed to load projects: ${(error as Error).message}`} />
    );
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-8">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Dashboard</h1>
          <p className="text-gray-500 mt-1">Manage your migration projects</p>
        </div>
        <Link
          to="/projects/new"
          className="inline-flex items-center gap-2 px-4 py-2.5 bg-indigo-600 text-white text-sm font-medium rounded-lg hover:bg-indigo-700 transition-colors"
        >
          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
          </svg>
          New Project
        </Link>
      </div>

      {!projects || projects.length === 0 ? (
        <EmptyState />
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {projects.map((project) => (
            <ProjectCard
              key={project.id}
              project={project}
              onRun={() => runMutation.mutate(project.id)}
              onDelete={() => {
                if (confirm(`Delete project "${project.name}"?`)) {
                  deleteMutation.mutate(project.id);
                }
              }}
              isRunning={runMutation.isPending}
            />
          ))}
        </div>
      )}
    </div>
  );
}

function ProjectCard({
  project,
  onRun,
  onDelete,
  isRunning,
}: {
  project: Project;
  onRun: () => void;
  onDelete: () => void;
  isRunning: boolean;
}) {
  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6 hover:shadow-md transition-shadow">
      <div className="flex items-start justify-between mb-3">
        <div>
          <h3 className="font-semibold text-gray-900">{project.name}</h3>
          {project.description && (
            <p className="text-sm text-gray-500 mt-1">{project.description}</p>
          )}
        </div>
        <span className={`px-2.5 py-0.5 rounded-full text-xs font-medium ${statusColors[project.status]}`}>
          {project.status}
        </span>
      </div>

      <div className="text-sm text-gray-500 space-y-1 mb-4">
        <div className="flex items-center gap-2">
          <span className="font-medium text-gray-600">Source:</span>
          <span>{project.source.connector} ({project.source.database})</span>
        </div>
        <div className="flex items-center gap-2">
          <span className="font-medium text-gray-600">Target:</span>
          <span>{project.target.connector} ({project.target.database})</span>
        </div>
        <div className="flex items-center gap-2">
          <span className="font-medium text-gray-600">Tables:</span>
          <span>{project.tables.length} selected</span>
        </div>
      </div>

      <div className="flex gap-2 pt-3 border-t border-gray-100">
        <button
          onClick={onRun}
          disabled={project.status === 'running' || isRunning}
          className="flex-1 inline-flex items-center justify-center gap-1.5 px-3 py-2 bg-indigo-600 text-white text-sm font-medium rounded-lg hover:bg-indigo-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
        >
          <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 20 20">
            <path d="M6.3 2.84A1.5 1.5 0 004 4.11v11.78a1.5 1.5 0 002.3 1.27l9.344-5.891a1.5 1.5 0 000-2.538L6.3 2.841z" />
          </svg>
          Run
        </button>
        <Link
          to={`/projects/${project.id}`}
          className="flex-1 inline-flex items-center justify-center px-3 py-2 border border-gray-300 text-gray-700 text-sm font-medium rounded-lg hover:bg-gray-50 transition-colors"
        >
          Edit
        </Link>
        <button
          onClick={onDelete}
          className="px-3 py-2 text-red-500 hover:text-red-700 hover:bg-red-50 rounded-lg transition-colors"
          title="Delete project"
        >
          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
          </svg>
        </button>
      </div>
    </div>
  );
}

function EmptyState() {
  return (
    <div className="text-center py-16 bg-white rounded-xl border border-gray-200">
      <svg className="w-16 h-16 mx-auto text-gray-300 mb-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1} d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4" />
      </svg>
      <h3 className="text-lg font-medium text-gray-900 mb-1">No projects yet</h3>
      <p className="text-gray-500 mb-6">Create your first migration project to get started.</p>
      <Link
        to="/projects/new"
        className="inline-flex items-center gap-2 px-4 py-2.5 bg-indigo-600 text-white text-sm font-medium rounded-lg hover:bg-indigo-700 transition-colors"
      >
        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
        </svg>
        Create Project
      </Link>
    </div>
  );
}

function LoadingState() {
  return (
    <div className="flex items-center justify-center py-16">
      <div className="flex items-center gap-3 text-gray-500">
        <svg className="animate-spin w-5 h-5" fill="none" viewBox="0 0 24 24">
          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
        </svg>
        Loading projects...
      </div>
    </div>
  );
}

function ErrorAlert({ message }: { message: string }) {
  return (
    <div className="bg-red-50 border border-red-200 rounded-lg p-4 flex items-start gap-3">
      <svg className="w-5 h-5 text-red-500 flex-shrink-0 mt-0.5" fill="currentColor" viewBox="0 0 20 20">
        <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.28 7.22a.75.75 0 00-1.06 1.06L8.94 10l-1.72 1.72a.75.75 0 101.06 1.06L10 11.06l1.72 1.72a.75.75 0 101.06-1.06L11.06 10l1.72-1.72a.75.75 0 00-1.06-1.06L10 8.94 8.28 7.22z" clipRule="evenodd" />
      </svg>
      <p className="text-sm text-red-700">{message}</p>
    </div>
  );
}

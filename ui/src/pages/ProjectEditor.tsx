import { useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  getProject,
  createProject,
  updateProject,
  getConnectors,
} from '../api/client';
import { ConnectionForm } from '../components/ConnectionForm';
import type { ConnectionConfig } from '../types';

const emptyConnection: ConnectionConfig = {
  connector: '',
  host: 'localhost',
  port: 5432,
  database: '',
  username_env: '',
  password_env: '',
  extra: {},
};

export function ProjectEditor() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const isNew = !id || id === 'new';

  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [source, setSource] = useState<ConnectionConfig>({ ...emptyConnection });
  const [target, setTarget] = useState<ConnectionConfig>({ ...emptyConnection });
  const [tables, setTables] = useState('');
  const [schedule, setSchedule] = useState('');
  const [formLoaded, setFormLoaded] = useState(false);

  // Load existing project
  useQuery({
    queryKey: ['project', id],
    queryFn: () => getProject(id!),
    enabled: !isNew,
    // populate form when data arrives
    select: (data) => {
      if (!formLoaded) {
        setName(data.name);
        setDescription(data.description);
        setSource(data.source);
        setTarget(data.target);
        setTables(data.tables.join('\n'));
        setSchedule(data.schedule ?? '');
        setFormLoaded(true);
      }
      return data;
    },
  });

  // Load connector list
  const { data: connectors } = useQuery({
    queryKey: ['connectors'],
    queryFn: getConnectors,
  });

  const connectorNames = connectors?.map((c) => c.name) ?? [
    'postgresql',
    'mysql',
    'mssql',
    'oracle',
    'sqlite',
  ];

  const saveMutation = useMutation({
    mutationFn: () => {
      const payload = {
        name,
        description,
        source,
        target,
        tables: tables
          .split('\n')
          .map((t) => t.trim())
          .filter(Boolean),
        type_mapping_overrides: [],
        hooks: [],
        schedule: schedule || null,
      };

      if (isNew) {
        return createProject(payload);
      }
      return updateProject(id!, payload);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['projects'] });
      navigate('/');
    },
  });

  return (
    <div>
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-gray-900">
          {isNew ? 'New Project' : 'Edit Project'}
        </h1>
        <p className="text-gray-500 mt-1">
          {isNew
            ? 'Configure a new migration project'
            : 'Update migration project settings'}
        </p>
      </div>

      <form
        onSubmit={(e) => {
          e.preventDefault();
          saveMutation.mutate();
        }}
        className="space-y-8 max-w-3xl"
      >
        {/* Project Details */}
        <section className="bg-white rounded-xl border border-gray-200 p-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">
            Project Details
          </h2>
          <div className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Project Name
              </label>
              <input
                type="text"
                value={name}
                onChange={(e) => setName(e.target.value)}
                required
                placeholder="My Migration"
                className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Description
              </label>
              <textarea
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                rows={2}
                placeholder="Optional description..."
                className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
              />
            </div>
          </div>
        </section>

        {/* Source Connection */}
        <section className="bg-white rounded-xl border border-gray-200 p-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">
            Source Database
          </h2>
          <ConnectionForm
            value={source}
            onChange={setSource}
            connectorOptions={connectorNames}
          />
        </section>

        {/* Target Connection */}
        <section className="bg-white rounded-xl border border-gray-200 p-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">
            Target Database
          </h2>
          <ConnectionForm
            value={target}
            onChange={setTarget}
            connectorOptions={connectorNames}
          />
        </section>

        {/* Tables */}
        <section className="bg-white rounded-xl border border-gray-200 p-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">Tables</h2>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Table names (one per line, leave blank for all tables)
            </label>
            <textarea
              value={tables}
              onChange={(e) => setTables(e.target.value)}
              rows={6}
              placeholder={"users\norders\nproducts"}
              className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
            />
          </div>
        </section>

        {/* Schedule */}
        <section className="bg-white rounded-xl border border-gray-200 p-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">
            Schedule (optional)
          </h2>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Cron expression
            </label>
            <input
              type="text"
              value={schedule}
              onChange={(e) => setSchedule(e.target.value)}
              placeholder="0 2 * * * (daily at 2 AM)"
              className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
            />
            <p className="mt-1 text-xs text-gray-400">
              Leave blank for manual-only execution
            </p>
          </div>
        </section>

        {/* Error display */}
        {saveMutation.isError && (
          <div className="bg-red-50 border border-red-200 rounded-lg p-4 flex items-start gap-3">
            <svg className="w-5 h-5 text-red-500 flex-shrink-0 mt-0.5" fill="currentColor" viewBox="0 0 20 20">
              <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.28 7.22a.75.75 0 00-1.06 1.06L8.94 10l-1.72 1.72a.75.75 0 101.06 1.06L10 11.06l1.72 1.72a.75.75 0 101.06-1.06L11.06 10l1.72-1.72a.75.75 0 00-1.06-1.06L10 8.94 8.28 7.22z" clipRule="evenodd" />
            </svg>
            <p className="text-sm text-red-700">
              {(saveMutation.error as Error).message}
            </p>
          </div>
        )}

        {/* Actions */}
        <div className="flex gap-3">
          <button
            type="submit"
            disabled={saveMutation.isPending}
            className="inline-flex items-center gap-2 px-6 py-2.5 bg-indigo-600 text-white text-sm font-medium rounded-lg hover:bg-indigo-700 disabled:opacity-50 transition-colors"
          >
            {saveMutation.isPending && (
              <svg className="animate-spin w-4 h-4" fill="none" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
              </svg>
            )}
            {isNew ? 'Create Project' : 'Save Changes'}
          </button>
          <button
            type="button"
            onClick={() => navigate('/')}
            className="px-6 py-2.5 border border-gray-300 text-gray-700 text-sm font-medium rounded-lg hover:bg-gray-50 transition-colors"
          >
            Cancel
          </button>
        </div>
      </form>
    </div>
  );
}

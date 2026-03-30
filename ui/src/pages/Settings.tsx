import { useState, useEffect } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getSettings, updateSettings, clearRunHistory } from '../api/client';
import type { Settings as SettingsType } from '../types';

const LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR'] as const;

export function Settings() {
  const queryClient = useQueryClient();
  const [form, setForm] = useState<SettingsType | null>(null);
  const [saved, setSaved] = useState(false);

  const { data: settings, isLoading, error } = useQuery({
    queryKey: ['settings'],
    queryFn: getSettings,
  });

  useEffect(() => {
    if (settings && !form) {
      setForm(settings);
    }
  }, [settings, form]);

  const saveMutation = useMutation({
    mutationFn: () => {
      if (!form) throw new Error('No form data');
      return updateSettings(form);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['settings'] });
      setSaved(true);
      setTimeout(() => setSaved(false), 3000);
    },
  });

  function update<K extends keyof SettingsType>(key: K, value: SettingsType[K]) {
    if (form) {
      setForm({ ...form, [key]: value });
    }
  }

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-16">
        <div className="flex items-center gap-3 text-gray-500">
          <svg className="animate-spin w-5 h-5" fill="none" viewBox="0 0 24 24">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
          </svg>
          Loading settings...
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-red-50 border border-red-200 rounded-lg p-4">
        <p className="text-sm text-red-700">
          Failed to load settings: {(error as Error).message}
        </p>
      </div>
    );
  }

  if (!form) return null;

  return (
    <div>
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-gray-900">Settings</h1>
        <p className="text-gray-500 mt-1">
          Global Bani configuration
        </p>
      </div>

      <form
        onSubmit={(e) => {
          e.preventDefault();
          saveMutation.mutate();
        }}
        className="space-y-8 max-w-2xl"
      >
        {/* Performance */}
        <section className="bg-white rounded-xl border border-gray-200 p-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-1">
            Performance
          </h2>
          <p className="text-sm text-gray-500 mb-4">
            Global defaults used when a project doesn&apos;t specify its own values.
          </p>
          <div className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Batch Size
              </label>
              <input
                type="number"
                value={form.batch_size}
                onChange={(e) =>
                  update('batch_size', parseInt(e.target.value) || 1000)
                }
                min={1000}
                max={500000}
                className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
              />
              <p className="mt-1 text-xs text-gray-400">
                Rows per batch during data transfer. Default: 100,000. Larger batches use more memory but reduce round trips. Range: 1,000 - 500,000.
              </p>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Max Workers
              </label>
              <input
                type="number"
                value={form.max_workers}
                onChange={(e) =>
                  update('max_workers', parseInt(e.target.value) || 1)
                }
                min={1}
                max={16}
                className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
              />
              <p className="mt-1 text-xs text-gray-400">
                Concurrent table transfers. Default: 4. Each worker uses its own database connection. Range: 1 - 16.
              </p>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Memory Limit (MB)
              </label>
              <input
                type="number"
                value={form.memory_limit_mb}
                onChange={(e) =>
                  update('memory_limit_mb', parseInt(e.target.value) || 256)
                }
                min={256}
                max={8192}
                className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
              />
              <p className="mt-1 text-xs text-gray-400">
                Memory budget for data buffering. Default: 2,048 MB. Range: 256 - 8,192 MB.
              </p>
            </div>
          </div>
        </section>

        {/* Logging */}
        <section className="bg-white rounded-xl border border-gray-200 p-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">Logging</h2>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Log Level
            </label>
            <select
              value={form.log_level}
              onChange={(e) =>
                update('log_level', e.target.value as SettingsType['log_level'])
              }
              className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
            >
              {LOG_LEVELS.map((level) => (
                <option key={level} value={level}>
                  {level}
                </option>
              ))}
            </select>
          </div>
        </section>

        {/* Checkpoints */}
        <section className="bg-white rounded-xl border border-gray-200 p-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">
            Checkpoints
          </h2>
          <div className="space-y-4">
            <div className="flex items-center gap-3">
              <input
                type="checkbox"
                id="checkpoint-enabled"
                checked={form.checkpoint_enabled}
                onChange={(e) =>
                  update('checkpoint_enabled', e.target.checked)
                }
                className="h-4 w-4 rounded border-gray-300 text-indigo-600 focus:ring-indigo-500"
              />
              <label
                htmlFor="checkpoint-enabled"
                className="text-sm font-medium text-gray-700"
              >
                Enable checkpoints for resumable migrations
              </label>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Checkpoint Directory
              </label>
              <input
                type="text"
                value={form.checkpoint_dir}
                onChange={(e) => update('checkpoint_dir', e.target.value)}
                placeholder=".bani/checkpoints"
                className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
              />
            </div>
          </div>
        </section>

        {/* Data Management */}
        <section className="bg-white rounded-xl border border-gray-200 p-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-2">
            Data Management
          </h2>
          <p className="text-sm text-gray-500 mb-4">
            Clear dashboard statistics and run history.
          </p>
          <ClearRunHistoryButton />
        </section>

        {/* Save feedback */}
        {saveMutation.isError && (
          <div className="bg-red-50 border border-red-200 rounded-lg p-4">
            <p className="text-sm text-red-700">
              {(saveMutation.error as Error).message}
            </p>
          </div>
        )}

        {saved && (
          <div className="bg-green-50 border border-green-200 rounded-lg p-4">
            <p className="text-sm text-green-700">Settings saved successfully.</p>
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
            Save Settings
          </button>
          <button
            type="button"
            onClick={() => setForm(settings ?? null)}
            disabled={JSON.stringify(form) === JSON.stringify(settings)}
            className="px-6 py-2.5 border border-gray-300 text-gray-700 text-sm font-medium rounded-lg hover:bg-gray-50 disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
          >
            Discard Changes
          </button>
        </div>
      </form>
    </div>
  );
}

function ClearRunHistoryButton() {
  const queryClient = useQueryClient();
  const [cleared, setCleared] = useState(false);

  const clearMutation = useMutation({
    mutationFn: clearRunHistory,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['recentRuns'] });
      queryClient.invalidateQueries({ queryKey: ['runSummary'] });
      setCleared(true);
      setTimeout(() => setCleared(false), 3000);
    },
  });

  return (
    <div className="flex items-center gap-3">
      <button
        type="button"
        onClick={() => {
          if (confirm('Clear all run history? This resets total runs, last run, lifetime rows, and recent runs on the dashboard.')) {
            clearMutation.mutate();
          }
        }}
        disabled={clearMutation.isPending}
        className="px-4 py-2 border border-gray-300 text-gray-700 text-sm font-medium rounded-lg hover:bg-gray-50 disabled:opacity-50 transition-colors"
      >
        {clearMutation.isPending ? 'Clearing...' : 'Clear Run History'}
      </button>
      {cleared && (
        <span className="text-sm text-green-600">Cleared</span>
      )}
      {clearMutation.isError && (
        <span className="text-sm text-red-600">
          {(clearMutation.error as Error).message}
        </span>
      )}
    </div>
  );
}

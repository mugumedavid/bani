import { useState, useEffect, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { validateMigration, startMigrationRun, getCheckpoint, type DryRunResult } from '../api/client';

interface RunMigrationDialogProps {
  projectName: string;
  open: boolean;
  defaultResume?: boolean;
  onClose: () => void;
}

export function RunMigrationDialog({
  projectName,
  open,
  defaultResume = false,
  onClose,
}: RunMigrationDialogProps) {
  const navigate = useNavigate();
  const [resume, setResume] = useState(false);
  const [dryRun, setDryRun] = useState(false);
  const [validating, setValidating] = useState(false);
  const [sourceStatus, setSourceStatus] = useState<string | null>(null);
  const [targetStatus, setTargetStatus] = useState<string | null>(null);
  const [starting, setStarting] = useState(false);
  const [startError, setStartError] = useState<string | null>(null);
  const [dryRunResult, setDryRunResult] = useState<DryRunResult | null>(null);
  const [hasCheckpoint, setHasCheckpoint] = useState(false);
  const [checkpointLabel, setCheckpointLabel] = useState('');

  const runValidation = useCallback(async () => {
    setValidating(true);
    setSourceStatus(null);
    setTargetStatus(null);
    setStartError(null);
    try {
      const result = await validateMigration(projectName);
      setSourceStatus(result.source);
      setTargetStatus(result.target);
    } catch (err) {
      setSourceStatus(`Validation request failed: ${(err as Error).message}`);
      setTargetStatus(`Validation request failed: ${(err as Error).message}`);
    } finally {
      setValidating(false);
    }
  }, [projectName]);

  useEffect(() => {
    if (open) {
      setResume(defaultResume);
      setDryRun(false);
      setStarting(false);
      setStartError(null);
      setDryRunResult(null);
      setHasCheckpoint(false);
      setCheckpointLabel('');
      runValidation();
      // Check for checkpoint
      getCheckpoint(projectName).then((info) => {
        if (info.exists) {
          setHasCheckpoint(true);
          setCheckpointLabel(
            `${info.tables_completed} / ${info.tables_total} tables completed`
          );
        }
      }).catch(() => { /* ignore */ });
    }
  }, [open, runValidation]);

  const bothOk = sourceStatus === 'ok' && targetStatus === 'ok';

  async function handleRun() {
    if (starting) return;
    setStarting(true);
    setStartError(null);
    setDryRunResult(null);
    try {
      const result = await startMigrationRun(projectName, { resume, dry_run: dryRun });
      if ('dry_run' in result && result.dry_run) {
        // Check for dry run error (missing/ambiguous tables)
        if ('error' in result && (result as any).status === 'error') {
          setStartError((result as any).error);
          setStarting(false);
          return;
        }
        // Show dry run results in the dialog
        setDryRunResult(result as DryRunResult);
        setStarting(false);
      } else {
        onClose();
        navigate(`/monitor?started=${encodeURIComponent(projectName)}`);
      }
    } catch (err) {
      setStartError((err as Error).message);
      setStarting(false);
    }
  }

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      {/* Backdrop */}
      <div
        className="absolute inset-0 bg-black/40"
        onClick={onClose}
      />

      {/* Dialog */}
      <div className={`relative bg-white rounded-xl shadow-xl w-full mx-4 p-6 ${dryRunResult ? 'max-w-2xl' : 'max-w-md'}`}>
        <h2 className="text-lg font-semibold text-gray-900 mb-1">
          {dryRunResult ? 'Dry Run Results' : 'Run Migration'}
        </h2>
        <p className="text-sm text-gray-500 mb-5">
          Project: <span className="font-medium text-gray-700">{projectName}</span>
        </p>

        {/* Dry run results view */}
        {dryRunResult ? (
          <div>
            <div className="grid grid-cols-3 gap-3 mb-4">
              <div className="bg-gray-50 rounded-lg p-3">
                <p className="text-xs text-gray-500">Source</p>
                <p className="text-sm font-semibold text-gray-900">{dryRunResult.source_dialect}</p>
              </div>
              <div className="bg-gray-50 rounded-lg p-3">
                <p className="text-xs text-gray-500">Target</p>
                <p className="text-sm font-semibold text-gray-900">{dryRunResult.target_dialect}</p>
              </div>
              <div className="bg-gray-50 rounded-lg p-3">
                <p className="text-xs text-gray-500">Estimated Rows</p>
                <p className="text-sm font-semibold text-gray-900">{dryRunResult.total_estimated_rows.toLocaleString()}</p>
              </div>
            </div>

            <div className="border border-gray-200 rounded-lg overflow-hidden mb-5">
              <div className="bg-gray-50 px-4 py-2 border-b border-gray-200">
                <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wide">
                  {dryRunResult.table_count} Tables
                </h3>
              </div>
              <div className="max-h-64 overflow-y-auto">
                <table className="w-full text-sm">
                  <thead className="bg-gray-50 sticky top-0">
                    <tr>
                      <th className="text-left px-4 py-2 text-xs font-medium text-gray-500">Table</th>
                      <th className="text-right px-4 py-2 text-xs font-medium text-gray-500">Columns</th>
                      <th className="text-right px-4 py-2 text-xs font-medium text-gray-500">Est. Rows</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100">
                    {dryRunResult.tables.map((t) => (
                      <tr key={t.name}>
                        <td className="px-4 py-1.5 font-mono text-xs text-gray-700">{t.name}</td>
                        <td className="px-4 py-1.5 text-right text-xs text-gray-500">{t.columns}</td>
                        <td className="px-4 py-1.5 text-right text-xs text-gray-500">
                          {t.estimated_rows ? t.estimated_rows.toLocaleString() : '—'}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>

            <div className="bg-green-50 border border-green-200 rounded-lg p-3 mb-5">
              <p className="text-sm text-green-700 font-medium">
                Dry run passed — no data was transferred
              </p>
            </div>

            <div className="flex items-center gap-3">
              <button
                onClick={() => {
                  setDryRunResult(null);
                  setDryRun(false);
                }}
                className="flex-1 inline-flex items-center justify-center gap-2 px-4 py-2.5 bg-indigo-600 text-white text-sm font-medium rounded-lg hover:bg-indigo-700 transition-colors"
              >
                Run Migration
              </button>
              <button
                onClick={onClose}
                className="px-4 py-2.5 border border-gray-300 text-gray-700 text-sm font-medium rounded-lg hover:bg-gray-50 transition-colors"
              >
                Close
              </button>
            </div>
          </div>
        ) : (
          <>
            {/* Options */}
            <div className="space-y-3 mb-5">
              {hasCheckpoint && (
                <label className="flex items-center gap-3 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={resume}
                    onChange={(e) => setResume(e.target.checked)}
                    className="rounded border-gray-300 text-indigo-600 focus:ring-indigo-500"
                  />
                  <div>
                    <span className="text-sm font-medium text-gray-700">Resume from checkpoint</span>
                    <p className="text-xs text-amber-600">{checkpointLabel}</p>
                  </div>
                </label>
              )}
              <label className="flex items-center gap-3 cursor-pointer">
                <input
                  type="checkbox"
                  checked={dryRun}
                  onChange={(e) => setDryRun(e.target.checked)}
                  className="rounded border-gray-300 text-indigo-600 focus:ring-indigo-500"
                />
                <div>
                  <span className="text-sm font-medium text-gray-700">Dry run</span>
                  <p className="text-xs text-gray-400">Validate and preview schema without transferring data</p>
                </div>
              </label>
            </div>

            {/* Connection validation */}
            <div className="bg-gray-50 rounded-lg p-4 mb-5">
              <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-3">
                Connection Test
              </h3>

              {validating ? (
                <div className="flex items-center gap-2 text-sm text-gray-500">
                  <svg
                    className="animate-spin w-4 h-4 text-indigo-500"
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
                  Testing connections...
                </div>
              ) : (
                <div className="space-y-2">
                  <ConnectionStatus label="Source" status={sourceStatus} />
                  <ConnectionStatus label="Target" status={targetStatus} />
                </div>
              )}
            </div>

            {/* Start error */}
            {startError && (
              <div className="bg-red-50 border border-red-200 rounded-lg p-3 mb-4">
                <p className="text-sm text-red-700">{startError}</p>
              </div>
            )}

            {/* Actions */}
            <div className="flex items-center gap-3">
              <button
                onClick={handleRun}
                disabled={!bothOk || validating || starting}
                className="flex-1 inline-flex items-center justify-center gap-2 px-4 py-2.5 bg-indigo-600 text-white text-sm font-medium rounded-lg hover:bg-indigo-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
              >
                {starting && (
                  <svg
                    className="animate-spin w-4 h-4"
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
                )}
                {starting ? (dryRun ? 'Analysing...' : 'Starting...') : (dryRun ? 'Run Dry Run' : 'Run Migration')}
              </button>

              {!validating && !bothOk && (
                <button
                  onClick={runValidation}
                  className="px-4 py-2.5 border border-gray-300 text-gray-700 text-sm font-medium rounded-lg hover:bg-gray-50 transition-colors"
                >
                  Retry
                </button>
              )}

              <button
                onClick={onClose}
                className="px-4 py-2.5 border border-gray-300 text-gray-700 text-sm font-medium rounded-lg hover:bg-gray-50 transition-colors"
              >
                Cancel
              </button>
            </div>
          </>
        )}
      </div>
    </div>
  );
}

function ConnectionStatus({
  label,
  status,
}: {
  label: string;
  status: string | null;
}) {
  if (status === null) return null;

  const isOk = status === 'ok';
  return (
    <div className="flex items-start gap-2">
      {isOk ? (
        <svg
          className="w-4 h-4 text-green-500 mt-0.5 flex-shrink-0"
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M5 13l4 4L19 7"
          />
        </svg>
      ) : (
        <svg
          className="w-4 h-4 text-red-500 mt-0.5 flex-shrink-0"
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M6 18L18 6M6 6l12 12"
          />
        </svg>
      )}
      <div className="min-w-0">
        <span className={`text-sm font-medium ${isOk ? 'text-green-700' : 'text-red-700'}`}>
          {label} {isOk ? 'connected' : 'failed'}
        </span>
        {!isOk && (
          <p className="text-xs text-red-500 mt-0.5 break-all">{status}</p>
        )}
      </div>
    </div>
  );
}

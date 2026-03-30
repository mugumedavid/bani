import { useState, useCallback } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  getProject,
  createProject,
  updateProject,
  getConnectors,
} from '../api/client';
import { ConnectionForm } from '../components/ConnectionForm';
import { RunMigrationDialog } from '../components/RunMigrationDialog';
import type { ConnectionConfig, SavedConnection } from '../types';

/* ── Helpers ───────────────────────────────────────────── */

function loadSavedConnections(): SavedConnection[] {
  try {
    const raw = sessionStorage.getItem('bani_saved_connections');
    if (raw) return JSON.parse(raw);
  } catch { /* ignore */ }
  return [];
}

const emptyConnection: ConnectionConfig = {
  name: '',
  connector: '',
  host: 'localhost',
  port: 5432,
  database: '',
  username_env: '',
  password_env: '',
  username_is_env: false,
  password_is_env: false,
  extra: {},
};

interface ProjectDetail {
  name: string;
  path: string;
  content: string;
}

interface FormState {
  name: string;
  description: string;
  source: ConnectionConfig;
  target: ConnectionConfig;
  tables: string;
  schedule: string;
}

/* ── BDL ↔ Form parsing ───────────────────────────────── */

function textOf(el: Element | null, tag: string): string {
  return el?.querySelector(tag)?.textContent?.trim() ?? '';
}

function parseCredential(raw: string): { value: string; isEnv: boolean } {
  // ${env:VAR_NAME} → env var mode
  const m = raw.match(/^\$\{env:(.+)\}$/);
  if (m) return { value: m[1], isEnv: true };
  // Anything else → direct value
  return { value: raw, isEnv: false };
}

function parseConnectionFromWrapper(wrapperEl: Element | null): ConnectionConfig {
  // Handles: <source connector="pg"><connection host="..." ... /></source>
  if (!wrapperEl) return { ...emptyConnection };
  const connEl = wrapperEl.querySelector('connection');
  const connector = wrapperEl.getAttribute('connector') ?? '';
  if (connEl) {
    const user = parseCredential(connEl.getAttribute('username') ?? '');
    const pass = parseCredential(connEl.getAttribute('password') ?? '');
    return {
      name: '',
      connector,
      host: connEl.getAttribute('host') || 'localhost',
      port: parseInt(connEl.getAttribute('port') ?? '') || 0,
      database: connEl.getAttribute('database') ?? '',
      username_env: user.value,
      password_env: pass.value,
      username_is_env: user.isEnv,
      password_is_env: pass.isEnv,
      extra: {},
    };
  }
  return { ...emptyConnection, connector };
}

function parseConnectionLegacy(el: Element | null): ConnectionConfig {
  // Handles: <connection type="source" connector="pg"><host>...</host>...</connection>
  if (!el) return { ...emptyConnection };
  const user = parseCredential(textOf(el, 'username'));
  const pass = parseCredential(textOf(el, 'password'));
  return {
    name: '',
    connector: el.getAttribute('connector') ?? '',
    host: textOf(el, 'host') || 'localhost',
    port: parseInt(textOf(el, 'port')) || 0,
    database: textOf(el, 'database'),
    username_env: user.value,
    password_env: pass.value,
    username_is_env: user.isEnv,
    password_is_env: pass.isEnv,
    extra: {},
  };
}

function parseBdlToForm(xml: string): FormState {
  try {
    const doc = new DOMParser().parseFromString(xml, 'text/xml');
    const projectEl = doc.querySelector('project');
    const scheduleEl = doc.querySelector('schedule cron') ?? doc.querySelector('cron');

    // Try new format: <source connector="..."><connection .../></source>
    const sourceWrapper = doc.querySelector('source[connector]');
    const targetWrapper = doc.querySelector('target[connector]');

    let source: ConnectionConfig;
    let target: ConnectionConfig;

    if (sourceWrapper || targetWrapper) {
      source = parseConnectionFromWrapper(sourceWrapper);
      target = parseConnectionFromWrapper(targetWrapper);
    } else {
      // Legacy format: <connection type="source" connector="...">
      source = parseConnectionLegacy(doc.querySelector('connection[type="source"]'));
      target = parseConnectionLegacy(doc.querySelector('connection[type="target"]'));
    }

    return {
      name: projectEl?.getAttribute('name') ?? '',
      description: projectEl?.getAttribute('description') ?? '',
      source,
      target,
      tables: '',
      schedule: scheduleEl?.textContent?.trim() ?? '',
    };
  } catch {
    return {
      name: '',
      description: '',
      source: { ...emptyConnection },
      target: { ...emptyConnection },
      tables: '',
      schedule: '',
    };
  }
}

function formToBdlXml(form: FormState): string {
  function credRef(value: string, isEnv: boolean): string {
    if (isEnv) return `\${env:${value}}`;
    return value; // direct value stored as-is (temp env var generated at runtime)
  }
  const s = form.source;
  const t = form.target;
  let xml = `<?xml version="1.0" encoding="UTF-8"?>
<bani schemaVersion="1.0">
  <project name="${form.name}" description="${form.description}"/>
  <source connector="${s.connector}">
    <connection host="${s.host}" port="${s.port}" database="${s.database}" username="${credRef(s.username_env, s.username_is_env)}" password="${credRef(s.password_env, s.password_is_env)}" />
  </source>
  <target connector="${t.connector}">
    <connection host="${t.host}" port="${t.port}" database="${t.database}" username="${credRef(t.username_env, t.username_is_env)}" password="${credRef(t.password_env, t.password_is_env)}" />
  </target>`;

  if (form.schedule) {
    xml += `
  <schedule enabled="true">
    <cron>${form.schedule}</cron>
  </schedule>`;
  }

  xml += `
</bani>`;
  return xml;
}

/* ── Connection Picker ─────────────────────────────────── */

function ConnectionPicker({
  label,
  onSelect,
}: {
  label: string;
  onSelect: (conn: ConnectionConfig) => void;
}) {
  const saved = loadSavedConnections();
  if (saved.length === 0) return null;
  return (
    <div className="mb-4">
      <label className="block text-sm font-medium text-gray-700 mb-1">{label}</label>
      <select
        defaultValue=""
        onChange={(e) => {
          const found = saved.find((s) => s.connection.name === e.target.value);
          if (found) onSelect(found.connection);
        }}
        className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500"
      >
        <option value="">Or pick from a saved connection...</option>
        {saved.map((s) => (
          <option key={s.connection.name} value={s.connection.name}>
            {s.connection.name} ({s.connection.connector} — {s.connection.host}:{s.connection.port}/{s.connection.database})
          </option>
        ))}
      </select>
    </div>
  );
}

/* ── Visual Form ───────────────────────────────────────── */

function VisualForm({
  form,
  setForm,
  connectorNames,
  isNew,
}: {
  form: FormState;
  setForm: (f: FormState) => void;
  connectorNames: string[];
  isNew: boolean;
}) {
  return (
    <div className="space-y-8">
      {/* Project Details */}
      <section className="bg-white rounded-xl border border-gray-200 p-6">
        <h2 className="text-lg font-semibold text-gray-900 mb-4">Project Details</h2>
        <div className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Project Name</label>
            <input
              type="text"
              value={form.name}
              onChange={(e) => isNew && setForm({ ...form, name: e.target.value })}
              readOnly={!isNew}
              required
              placeholder="My Migration"
              className={`w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500 ${!isNew ? 'bg-gray-50 text-gray-500 cursor-not-allowed' : ''}`}
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Description</label>
            <textarea
              value={form.description}
              onChange={(e) => setForm({ ...form, description: e.target.value })}
              rows={2}
              placeholder="Optional description..."
              className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500"
            />
          </div>
        </div>
      </section>

      {/* Source */}
      <section className="bg-white rounded-xl border border-gray-200 p-6">
        <h2 className="text-lg font-semibold text-gray-900 mb-4">Source Database</h2>
        <ConnectionPicker label="Use a saved connection" onSelect={(c) => setForm({ ...form, source: c })} />
        <ConnectionForm
          value={form.source}
          onChange={(s) => setForm({ ...form, source: s })}
          connectorOptions={connectorNames}
        />
      </section>

      {/* Target */}
      <section className="bg-white rounded-xl border border-gray-200 p-6">
        <h2 className="text-lg font-semibold text-gray-900 mb-4">Target Database</h2>
        <ConnectionPicker label="Use a saved connection" onSelect={(c) => setForm({ ...form, target: c })} />
        <ConnectionForm
          value={form.target}
          onChange={(t) => setForm({ ...form, target: t })}
          connectorOptions={connectorNames}
        />
      </section>

      {/* Tables */}
      <section className="bg-white rounded-xl border border-gray-200 p-6">
        <h2 className="text-lg font-semibold text-gray-900 mb-4">Tables</h2>
        <label className="block text-sm font-medium text-gray-700 mb-1">
          Table names (one per line, leave blank for all)
        </label>
        <textarea
          value={form.tables}
          onChange={(e) => setForm({ ...form, tables: e.target.value })}
          rows={6}
          placeholder={"users\norders\nproducts"}
          className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-indigo-500"
        />
      </section>

      {/* Schedule */}
      <section className="bg-white rounded-xl border border-gray-200 p-6">
        <h2 className="text-lg font-semibold text-gray-900 mb-4">Schedule (optional)</h2>
        <label className="block text-sm font-medium text-gray-700 mb-1">Cron expression</label>
        <input
          type="text"
          value={form.schedule}
          onChange={(e) => setForm({ ...form, schedule: e.target.value })}
          placeholder="0 2 * * * (daily at 2 AM)"
          className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-indigo-500"
        />
        <p className="mt-1 text-xs text-gray-400">Leave blank for manual-only execution</p>
      </section>
    </div>
  );
}

/* ── Main Editor ───────────────────────────────────────── */

export function ProjectEditor() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const isNew = !id || id === 'new';

  const [activeTab, setActiveTab] = useState<'visual' | 'source'>('visual');
  const [form, setForm] = useState<FormState>({
    name: '',
    description: '',
    source: { ...emptyConnection },
    target: { ...emptyConnection },
    tables: '',
    schedule: '',
  });
  const [bdlContent, setBdlContent] = useState('');
  const [projectLoaded, setProjectLoaded] = useState(false);
  const [showRunDialog, setShowRunDialog] = useState(false);
  const [runAfterSave, setRunAfterSave] = useState(false);

  // Load existing project
  const { data: existingProject } = useQuery<ProjectDetail>({
    queryKey: ['project', id],
    queryFn: () => getProject(id!) as unknown as Promise<ProjectDetail>,
    enabled: !isNew,
  });

  // Populate form when data arrives
  if (existingProject && !projectLoaded) {
    setBdlContent(existingProject.content);
    const parsed = parseBdlToForm(existingProject.content);
    parsed.name = existingProject.name; // always use filename as name
    setForm(parsed);
    setProjectLoaded(true);
  }

  // Connector list
  const { data: connectors } = useQuery({
    queryKey: ['connectors'],
    queryFn: getConnectors,
  });
  const connectorNames = connectors?.map((c) => c.name) ?? [
    'postgresql', 'mysql', 'mssql', 'oracle', 'sqlite',
  ];

  // Tab switching with sync
  const switchToSource = useCallback(() => {
    setBdlContent(formToBdlXml(form));
    setActiveTab('source');
  }, [form]);

  const switchToVisual = useCallback(() => {
    const parsed = parseBdlToForm(bdlContent);
    setForm(parsed);
    setActiveTab('visual');
  }, [bdlContent]);

  // Save
  const saveMutation = useMutation({
    mutationFn: () => {
      const content = activeTab === 'visual' ? formToBdlXml(form) : bdlContent;
      const projectName = form.name || id || 'untitled';

      if (isNew) {
        return createProject({
          name: projectName,
          content,
        } as any);
      }
      return updateProject(id!, { content } as any);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['projects'] });
      queryClient.invalidateQueries({ queryKey: ['project', id] });
      if (runAfterSave) {
        setRunAfterSave(false);
        setShowRunDialog(true);
      } else {
        navigate('/');
      }
    },
  });

  return (
    <div>
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-gray-900">
          {isNew ? 'New Project' : 'Edit Project'}
        </h1>
        <p className="text-gray-500 mt-1">
          {isNew
            ? 'Configure a new migration project'
            : <>Editing <span className="font-medium">{form.name || id}</span></>}
        </p>
      </div>

      {/* Tab bar */}
      <div className="flex gap-1 mb-6 bg-gray-100 rounded-lg p-1 w-fit">
        <button
          onClick={activeTab === 'source' ? switchToVisual : undefined}
          className={`px-4 py-1.5 text-sm font-medium rounded-md transition-colors ${
            activeTab === 'visual'
              ? 'bg-white text-gray-900 shadow-sm'
              : 'text-gray-500 hover:text-gray-700'
          }`}
        >
          Visual
        </button>
        <button
          onClick={activeTab === 'visual' ? switchToSource : undefined}
          className={`px-4 py-1.5 text-sm font-medium rounded-md transition-colors ${
            activeTab === 'source'
              ? 'bg-white text-gray-900 shadow-sm'
              : 'text-gray-500 hover:text-gray-700'
          }`}
        >
          BDL Source
        </button>
      </div>

      {/* Warning when switching */}
      {activeTab === 'source' && !isNew && (
        <div className="mb-4 bg-amber-50 border border-amber-200 rounded-lg px-4 py-2">
          <p className="text-xs text-amber-700">
            Editing raw BDL. Unsupported elements will be preserved. Switch to Visual to use the form editor.
          </p>
        </div>
      )}

      <form
        onSubmit={(e) => { e.preventDefault(); saveMutation.mutate(); }}
        className="max-w-3xl"
      >
        {activeTab === 'visual' ? (
          <VisualForm form={form} setForm={setForm} connectorNames={connectorNames} isNew={isNew} />
        ) : (
          <section className="bg-white rounded-xl border border-gray-200 p-6">
            <textarea
              value={bdlContent}
              onChange={(e) => setBdlContent(e.target.value)}
              rows={24}
              className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-indigo-500"
              spellCheck={false}
            />
          </section>
        )}

        {saveMutation.isError && (
          <div className="mt-6 bg-red-50 border border-red-200 rounded-lg p-4">
            <p className="text-sm text-red-700">{(saveMutation.error as Error).message}</p>
          </div>
        )}

        <div className="flex gap-3 mt-6">
          <button
            type="submit"
            disabled={saveMutation.isPending}
            className="inline-flex items-center gap-2 px-6 py-2.5 bg-indigo-600 text-white text-sm font-medium rounded-lg hover:bg-indigo-700 disabled:opacity-50 transition-colors"
          >
            {saveMutation.isPending && !runAfterSave && (
              <svg className="animate-spin w-4 h-4" fill="none" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
              </svg>
            )}
            {isNew ? 'Create Project' : 'Save Changes'}
          </button>
          <button
            type="button"
            disabled={saveMutation.isPending}
            onClick={() => {
              setRunAfterSave(true);
              saveMutation.mutate();
            }}
            className="inline-flex items-center gap-2 px-6 py-2.5 bg-green-600 text-white text-sm font-medium rounded-lg hover:bg-green-700 disabled:opacity-50 transition-colors"
          >
            {saveMutation.isPending && runAfterSave && (
              <svg className="animate-spin w-4 h-4" fill="none" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
              </svg>
            )}
            Save &amp; Run
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

      {/* Run Migration Dialog */}
      <RunMigrationDialog
        projectName={form.name || id || 'untitled'}
        open={showRunDialog}
        onClose={() => setShowRunDialog(false)}
      />
    </div>
  );
}

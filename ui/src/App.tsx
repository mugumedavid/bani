import { useState, useEffect, Component, type ReactNode } from 'react';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { Layout } from './components/Layout';
import { Dashboard } from './pages/Dashboard';
import { ProjectEditor } from './pages/ProjectEditor';
import { SchemaBrowser } from './pages/SchemaBrowser';
import { MigrationMonitor } from './pages/MigrationMonitor';
import { RunHistory } from './pages/RunHistory';
import { ConnectorCatalog } from './pages/ConnectorCatalog';
import { Settings } from './pages/Settings';
import { useAuth } from './hooks/useAuth';

/* ── Error Boundary ─────────────────────────────────────── */

interface EBProps { children: ReactNode }
interface EBState { error: Error | null }

class ErrorBoundary extends Component<EBProps, EBState> {
  state: EBState = { error: null };

  static getDerivedStateFromError(error: Error) {
    return { error };
  }

  render() {
    if (this.state.error) {
      return (
        <div className="min-h-screen flex items-center justify-center bg-gray-100">
          <div className="bg-white p-8 rounded-lg shadow-md w-full max-w-md">
            <h1 className="text-xl font-bold text-red-600 mb-2">Something went wrong</h1>
            <p className="text-gray-600 mb-4 text-sm font-mono break-all">
              {this.state.error.message}
            </p>
            <div className="flex gap-3">
              <button
                onClick={() => { this.setState({ error: null }); window.location.href = '/'; }}
                className="px-4 py-2 bg-indigo-600 text-white rounded-md hover:bg-indigo-700 text-sm"
              >
                Go to Dashboard
              </button>
              <button
                onClick={() => {
                  sessionStorage.removeItem('bani_auth_token');
                  window.location.href = '/';
                }}
                className="px-4 py-2 bg-gray-200 text-gray-700 rounded-md hover:bg-gray-300 text-sm"
              >
                Logout &amp; Retry
              </button>
            </div>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}

/* ── Login Page ─────────────────────────────────────────── */

function LoginPage() {
  const { login } = useAuth();
  const [token, setToken] = useState('');

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (token.trim()) {
      login(token.trim());
    }
  }

  return (
    <div className="min-h-screen flex items-center justify-center bg-gray-100">
      <div className="bg-white p-8 rounded-lg shadow-md w-full max-w-md">
        <h1 className="text-2xl font-bold text-gray-900 mb-2">Bani</h1>
        <p className="text-gray-600 mb-6">
          Paste the auth token from the terminal where you ran{' '}
          <code className="bg-gray-100 px-1 rounded text-sm">bani ui</code>
        </p>
        <form onSubmit={handleSubmit}>
          <input
            type="text"
            value={token}
            onChange={(e) => setToken(e.target.value)}
            placeholder="Auth token"
            className="w-full border border-gray-300 rounded-md px-3 py-2 mb-4 focus:outline-none focus:ring-2 focus:ring-indigo-500"
            autoFocus
          />
          <button
            type="submit"
            className="w-full bg-indigo-600 text-white py-2 rounded-md hover:bg-indigo-700 transition"
          >
            Login
          </button>
        </form>
      </div>
    </div>
  );
}

/* ── App ────────────────────────────────────────────────── */

export default function App() {
  const { isAuthenticated, login } = useAuth();

  // Restore token from sessionStorage on mount
  useEffect(() => {
    const stored = sessionStorage.getItem('bani_auth_token');
    if (stored) {
      login(stored);
    }
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  if (!isAuthenticated) {
    return <LoginPage />;
  }

  return (
    <ErrorBoundary>
      <BrowserRouter>
        <Routes>
          <Route element={<Layout />}>
            <Route index element={<Dashboard />} />
            <Route path="projects/new" element={<ProjectEditor />} />
            <Route path="projects/:id" element={<ProjectEditor />} />
            <Route path="schema" element={<SchemaBrowser />} />
            <Route path="monitor" element={<MigrationMonitor />} />
            <Route path="history" element={<RunHistory />} />
            <Route path="connectors" element={<ConnectorCatalog />} />
            <Route path="settings" element={<Settings />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </ErrorBoundary>
  );
}

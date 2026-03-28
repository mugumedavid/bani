import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { Layout } from './components/Layout';
import { Dashboard } from './pages/Dashboard';
import { ProjectEditor } from './pages/ProjectEditor';
import { SchemaBrowser } from './pages/SchemaBrowser';
import { MigrationMonitor } from './pages/MigrationMonitor';
import { RunHistory } from './pages/RunHistory';
import { ConnectorCatalog } from './pages/ConnectorCatalog';
import { Settings } from './pages/Settings';

export default function App() {
  return (
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
  );
}

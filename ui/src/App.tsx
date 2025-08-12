import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import { Layout } from './components/layout/Layout';
import { Dashboard } from './pages/Dashboard';
import { JobsPage } from './pages/JobsPage';
import { JobDetailPage } from './pages/JobDetailPage';
import { RunsPage } from './pages/RunsPage';
import { RunDetailPage } from './pages/RunDetailPage';
import { ConfigurePage } from './pages/ConfigurePage';

function App() {
  return (
    <Router>
      <Layout>
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/jobs" element={<JobsPage />} />
          <Route path="/jobs/:jobName" element={<JobDetailPage />} />
          <Route path="/runs" element={<RunsPage />} />
          <Route path="/runs/:jobName/:runId" element={<RunDetailPage />} />
          <Route path="/schedule" element={
            <div className="p-8">
              <div className="text-center py-12">
                <h1 className="text-2xl font-bold text-gray-900 mb-4">Schedule</h1>
                <p className="text-gray-600">
                  Schedule management coming soon...
                </p>
              </div>
            </div>
          } />
          <Route path="/configure" element={<ConfigurePage />} />
        </Routes>
      </Layout>
    </Router>
  );
}

export default App;

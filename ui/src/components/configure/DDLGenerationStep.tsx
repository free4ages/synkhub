import React, { useState } from 'react';
import { Card, CardHeader, CardTitle, CardContent } from '../ui/Card';
import { DatabaseConnection } from '../../types/configure';

interface DDLGenerationStepProps {
  destinationConnection: DatabaseConnection | null;
  onDestinationConnectionChange: (connection: DatabaseConnection | null) => void;
  generatedDDL: string;
  configYaml: string;
  configJson: string;
}

export const DDLGenerationStep: React.FC<DDLGenerationStepProps> = ({
  destinationConnection,
  onDestinationConnectionChange,
  generatedDDL,
  configYaml,
  configJson
}) => {
  const [activeTab, setActiveTab] = useState<'ddl' | 'yaml' | 'json'>('ddl');

  const handleConnectionChange = (field: keyof DatabaseConnection, value: string | number) => {
    const updatedConnection: DatabaseConnection = {
      type: 'postgres',
      host: 'localhost',
      port: 5432,
      database: '',
      user: '',
      password: '',
      ...destinationConnection,
      [field]: value
    };
    onDestinationConnectionChange(updatedConnection);
  };

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text).then(() => {
      alert('Copied to clipboard!');
    });
  };

  const downloadFile = (content: string, filename: string) => {
    const blob = new Blob([content], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Destination Database Connection</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Database Type
              </label>
              <select
                value={destinationConnection?.type || 'postgres'}
                onChange={(e) => handleConnectionChange('type', e.target.value)}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                <option value="postgres">PostgreSQL</option>
                <option value="clickhouse">ClickHouse</option>
                <option value="starrocks">StarRocks</option>
                <option value="mysql">MySQL</option>
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Host
              </label>
              <input
                type="text"
                value={destinationConnection?.host || ''}
                onChange={(e) => handleConnectionChange('host', e.target.value)}
                placeholder="localhost"
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Port
              </label>
              <input
                type="number"
                value={destinationConnection?.port || ''}
                onChange={(e) => handleConnectionChange('port', parseInt(e.target.value) || 0)}
                placeholder="5432"
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Database Name
              </label>
              <input
                type="text"
                value={destinationConnection?.database || ''}
                onChange={(e) => handleConnectionChange('database', e.target.value)}
                placeholder="destination_db"
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Username
              </label>
              <input
                type="text"
                value={destinationConnection?.user || ''}
                onChange={(e) => handleConnectionChange('user', e.target.value)}
                placeholder="username"
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Password
              </label>
              <input
                type="password"
                value={destinationConnection?.password || ''}
                onChange={(e) => handleConnectionChange('password', e.target.value)}
                placeholder="password"
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>

            {(destinationConnection?.type === 'postgres' || destinationConnection?.type === 'mysql') && (
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Schema (Optional)
                </label>
                <input
                  type="text"
                  value={destinationConnection?.db_schema || ''}
                  onChange={(e) => handleConnectionChange('db_schema', e.target.value)}
                  placeholder="public"
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
            )}
          </div>
        </CardContent>
      </Card>

      {generatedDDL && (
        <Card>
          <CardHeader>
            <CardTitle>Generated Output</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="mb-4">
              <div className="flex space-x-1 border-b border-gray-200">
                <button
                  onClick={() => setActiveTab('ddl')}
                  className={`px-4 py-2 text-sm font-medium rounded-t-lg ${
                    activeTab === 'ddl'
                      ? 'bg-blue-500 text-white'
                      : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                  }`}
                >
                  DDL
                </button>
                <button
                  onClick={() => setActiveTab('yaml')}
                  className={`px-4 py-2 text-sm font-medium rounded-t-lg ${
                    activeTab === 'yaml'
                      ? 'bg-blue-500 text-white'
                      : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                  }`}
                >
                  YAML Config
                </button>
                <button
                  onClick={() => setActiveTab('json')}
                  className={`px-4 py-2 text-sm font-medium rounded-t-lg ${
                    activeTab === 'json'
                      ? 'bg-blue-500 text-white'
                      : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                  }`}
                >
                  JSON Config
                </button>
              </div>
            </div>

            <div className="relative">
              <div className="absolute top-2 right-2 flex space-x-2">
                <button
                  onClick={() => {
                    const content = activeTab === 'ddl' ? generatedDDL : activeTab === 'yaml' ? configYaml : configJson;
                    const filename = activeTab === 'ddl' ? 'create_table.sql' : activeTab === 'yaml' ? 'config.yaml' : 'config.json';
                    downloadFile(content, filename);
                  }}
                  className="px-3 py-1 bg-green-500 text-white text-xs rounded hover:bg-green-600"
                >
                  Download
                </button>
                <button
                  onClick={() => {
                    const content = activeTab === 'ddl' ? generatedDDL : activeTab === 'yaml' ? configYaml : configJson;
                    copyToClipboard(content);
                  }}
                  className="px-3 py-1 bg-blue-500 text-white text-xs rounded hover:bg-blue-600"
                >
                  Copy
                </button>
              </div>

              <pre className="bg-gray-900 text-green-400 p-4 rounded-md overflow-x-auto text-sm">
                <code>
                  {activeTab === 'ddl' && generatedDDL}
                  {activeTab === 'yaml' && configYaml}
                  {activeTab === 'json' && configJson}
                </code>
              </pre>
            </div>
          </CardContent>
        </Card>
      )}

      <div className="bg-green-50 border border-green-200 rounded-md p-4">
        <div className="flex">
          <div className="flex-shrink-0">
            <svg className="h-5 w-5 text-green-400" viewBox="0 0 20 20" fill="currentColor">
              <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
            </svg>
          </div>
          <div className="ml-3">
            <h3 className="text-sm font-medium text-green-800">
              Migration Ready
            </h3>
            <div className="mt-2 text-sm text-green-700">
              <p>
                Your schema migration configuration has been generated successfully! You can now:
              </p>
              <ul className="list-disc list-inside mt-2 space-y-1">
                <li>Use the generated DDL to create the destination table</li>
                <li>Save the configuration files for your sync tool</li>
                <li>Configure your destination database connection</li>
                <li>Start the data migration process</li>
              </ul>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

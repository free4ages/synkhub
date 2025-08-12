import React from 'react';
import { Card, CardHeader, CardTitle, CardContent } from '../ui/Card';
import { DatabaseConnection } from '../../types/configure';

interface ConnectionStepProps {
  sourceConnection: DatabaseConnection | null;
  onSourceConnectionChange: (connection: DatabaseConnection | null) => void;
  tableName: string;
  onTableNameChange: (tableName: string) => void;
}

export const ConnectionStep: React.FC<ConnectionStepProps> = ({
  sourceConnection,
  onSourceConnectionChange,
  tableName,
  onTableNameChange
}) => {
  const handleConnectionChange = (field: keyof DatabaseConnection, value: string | number) => {
    const updatedConnection: DatabaseConnection = {
      type: 'postgres',
      host: 'localhost',
      port: 5432,
      database: '',
      user: '',
      password: '',
      ...sourceConnection,
      [field]: value
    };
    onSourceConnectionChange(updatedConnection);
  };

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Source Database Connection</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Database Type
              </label>
              <select
                value={sourceConnection?.type || 'postgres'}
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
                value={sourceConnection?.host || ''}
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
                value={sourceConnection?.port || ''}
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
                value={sourceConnection?.database || ''}
                onChange={(e) => handleConnectionChange('database', e.target.value)}
                placeholder="mydatabase"
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Username
              </label>
              <input
                type="text"
                value={sourceConnection?.user || ''}
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
                value={sourceConnection?.password || ''}
                onChange={(e) => handleConnectionChange('password', e.target.value)}
                placeholder="password"
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>

            {(sourceConnection?.type === 'postgres' || sourceConnection?.type === 'mysql') && (
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Schema (Optional)
                </label>
                <input
                  type="text"
                  value={sourceConnection?.db_schema || ''}
                  onChange={(e) => handleConnectionChange('db_schema', e.target.value)}
                  placeholder="public"
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
            )}
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Table Information</CardTitle>
        </CardHeader>
        <CardContent>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Table Name
            </label>
            <input
              type="text"
              value={tableName}
              onChange={(e) => onTableNameChange(e.target.value)}
              placeholder="users"
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
            <p className="text-sm text-gray-500 mt-1">
              Enter the name of the table you want to migrate
            </p>
          </div>
        </CardContent>
      </Card>

      <div className="bg-blue-50 border border-blue-200 rounded-md p-4">
        <div className="flex">
          <div className="flex-shrink-0">
            <svg className="h-5 w-5 text-blue-400" viewBox="0 0 20 20" fill="currentColor">
              <path fillRule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clipRule="evenodd" />
            </svg>
          </div>
          <div className="ml-3">
            <h3 className="text-sm font-medium text-blue-800">
              Connection Information
            </h3>
            <div className="mt-2 text-sm text-blue-700">
              <p>
                Provide the connection details for your source database. The system will use these credentials to extract the table schema.
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

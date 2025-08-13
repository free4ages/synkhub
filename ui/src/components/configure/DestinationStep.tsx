import React from 'react';
import { Card, CardHeader, CardTitle, CardContent } from '../ui/Card';
import { DatabaseConnection } from '../../types/configure';

interface DestinationStepProps {
  destinationConnection: DatabaseConnection | null;
  onDestinationConnectionChange: (connection: DatabaseConnection | null) => void;
  destinationTableName: string;
  onDestinationTableNameChange: (tableName: string) => void;
  destinationSchema: string;
  onDestinationSchemaChange: (schema: string) => void;
}

export const DestinationStep: React.FC<DestinationStepProps> = ({
  destinationConnection,
  onDestinationConnectionChange,
  destinationTableName,
  onDestinationTableNameChange,
  destinationSchema,
  onDestinationSchemaChange
}) => {
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
              <p className="text-xs text-gray-500 mt-1">
                Database schema name (e.g., public, dbo, default). Leave empty to use default schema.
              </p>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Destination Table Information</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Table Name
              </label>
              <input
                type="text"
                value={destinationTableName}
                onChange={(e) => onDestinationTableNameChange(e.target.value)}
                placeholder="destination_table"
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
              <p className="text-xs text-gray-500 mt-1">
                Name of the destination table to be created
              </p>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Schema (Optional)
              </label>
              <input
                type="text"
                value={destinationSchema}
                onChange={(e) => onDestinationSchemaChange(e.target.value)}
                placeholder="public"
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
              <p className="text-xs text-gray-500 mt-1">
                Schema for the destination table. Leave empty to use default schema.
              </p>
            </div>
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
              Destination Database
            </h3>
            <div className="mt-2 text-sm text-blue-700">
              <p>
                Configure the connection details for your destination database where the migrated data will be stored.
                This database will be used to generate the DDL for creating the destination table.
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

import React, { useState } from 'react';
import { Card, CardHeader, CardTitle, CardContent } from '../ui/Card';
import { DatabaseConnection, TableInfo } from '../../types/configure';

interface ConnectionStepProps {
  sourceConnection: DatabaseConnection | null;
  onSourceConnectionChange: (connection: DatabaseConnection | null) => void;
  tableName: string;
  onTableNameChange: (tableName: string) => void;
  // New props for multiple tables
  tables: TableInfo[];
  onTablesChange: (tables: TableInfo[]) => void;
}

export const ConnectionStep: React.FC<ConnectionStepProps> = ({
  sourceConnection,
  onSourceConnectionChange,
  tableName,
  onTableNameChange,
  tables,
  onTablesChange
}) => {
  const [showSuccessMessage, setShowSuccessMessage] = useState(false);

  const generateAlias = (tableName: string, existingAliases: string[]): string => {
    // Generate alias from table name (first letter of each word)
    const words = tableName.split('_');
    let alias = words.map(word => word.charAt(0)).join('').toLowerCase();
    
    // If alias already exists, add a number
    let counter = 1;
    let finalAlias = alias;
    while (existingAliases.includes(finalAlias)) {
      finalAlias = `${alias}${counter}`;
      counter++;
    }
    
    return finalAlias;
  };

  const handleFillDefaultValues = () => {
    // Fill default database connection
    const defaultConnection: DatabaseConnection = {
      type: 'postgres',
      host: 'localhost',
      port: 5432,
      database: 'synctool',
      user: 'rohitanand',
      password: '',
      db_schema: 'public'
    };
    onSourceConnectionChange(defaultConnection);
    
    // Fill default table name
    onTableNameChange('users');
    
    // Fill default additional table
    const defaultTable: TableInfo = {
      table_name: 'user_profiles',
      alias: 'up',
      schema_name: 'public'
    };
    onTablesChange([defaultTable]);
    
    // Show success message
    setShowSuccessMessage(true);
    setTimeout(() => setShowSuccessMessage(false), 3000);
  };

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

  const handleAddTable = () => {
    const newTable: TableInfo = {
      table_name: '',
      alias: '',
      schema_name: sourceConnection?.db_schema || 'public'
    };
    onTablesChange([...tables, newTable]);
  };

  const handleRemoveTable = (index: number) => {
    const updatedTables = tables.filter((_, i) => i !== index);
    onTablesChange(updatedTables);
  };

  const handleTableChange = (index: number, field: keyof TableInfo, value: string) => {
    const updatedTables = [...tables];
    updatedTables[index] = { ...updatedTables[index], [field]: value };
    
    // Auto-generate alias if table name changes
    if (field === 'table_name' && value) {
      const existingAliases = updatedTables.map(t => t.alias).filter(a => a);
      const newAlias = generateAlias(value, existingAliases);
      updatedTables[index].alias = newAlias;
    }
    
    onTablesChange(updatedTables);
  };

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Source Database Connection</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex justify-end mb-4">
            <button
              type="button"
              onClick={handleFillDefaultValues}
              className="px-4 py-2 bg-green-600 text-white rounded-md hover:bg-green-700 flex items-center space-x-2"
            >
              <svg className="h-4 w-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 6v6m0 0v6m0-6h6m-6 0H6" />
              </svg>
              <span>Fill Default Values</span>
            </button>
          </div>

          {showSuccessMessage && (
            <div className="bg-green-100 border border-green-400 text-green-700 px-4 py-3 rounded mb-4">
              <div className="flex items-center">
                <svg className="h-5 w-5 mr-2" fill="currentColor" viewBox="0 0 20 20">
                  <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
                </svg>
                <span>Default values have been filled successfully!</span>
              </div>
            </div>
          )}

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
              <p className="text-xs text-gray-500 mt-1">
                Database schema name (e.g., public, dbo, default). Leave empty to use default schema.
              </p>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Table Information</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          {/* Single table mode (existing) */}
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

          {/* Multiple tables section */}
          <div className="border-t pt-4">
            <div className="flex justify-between items-center mb-4">
              <h3 className="text-lg font-medium">Additional Tables (for joins)</h3>
              <button
                type="button"
                onClick={handleAddTable}
                className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700"
              >
                Add Table
              </button>
            </div>

            {tables.map((table, index) => (
              <div key={index} className="border border-gray-200 rounded-md p-4 mb-4">
                <div className="flex justify-between items-center mb-4">
                  <h4 className="text-lg font-medium">Table {index + 1}</h4>
                  <button
                    type="button"
                    onClick={() => handleRemoveTable(index)}
                    className="text-red-600 hover:text-red-800 text-sm font-medium"
                  >
                    Remove
                  </button>
                </div>
                
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Table Name
                    </label>
                    <input
                      type="text"
                      value={table.table_name}
                      onChange={(e) => handleTableChange(index, 'table_name', e.target.value)}
                      placeholder="user_profiles"
                      className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                    />
                  </div>
                  
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Alias
                    </label>
                    <input
                      type="text"
                      value={table.alias}
                      onChange={(e) => handleTableChange(index, 'alias', e.target.value)}
                      placeholder="p"
                      className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                    />
                    <p className="text-xs text-gray-500 mt-1">
                      Auto-generated from table name
                    </p>
                  </div>
                  
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Schema (Optional)
                    </label>
                    <input
                      type="text"
                      value={table.schema_name || ''}
                      onChange={(e) => handleTableChange(index, 'schema_name', e.target.value)}
                      placeholder="public"
                      className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                    />
                  </div>
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      <div className="bg-green-50 border border-green-200 rounded-md p-4">
        <div className="flex">
          <div className="flex-shrink-0">
            <svg className="h-5 w-5 text-green-400" viewBox="0 0 20 20" fill="currentColor">
              <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
            </svg>
          </div>
          <div className="ml-3">
            <h3 className="text-sm font-medium text-green-800">
              Quick Setup
            </h3>
            <div className="mt-2 text-sm text-green-700">
              <p>
                Click the "Fill Default Values" button to quickly populate the form with common configuration values.
                <br />
                <strong>Default values include:</strong> PostgreSQL on localhost:5432, database: synctool, username: rohitanand, main table: users, additional table: user_profiles (alias: up)
              </p>
            </div>
          </div>
        </div>
      </div>

      <div className="bg-blue-50 border border-blue-200 rounded-md p-4">
        <div className="flex">
          <div className="flex-shrink-0">
            <svg className="h-5 w-5 text-blue-400" viewBox="0 0 20 20" fill="currentColor">
              <path fillRule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clipRule="evenodd" />
            </svg>
          </div>
          <div className="ml-3">
            <h3 className="text-sm font-medium text-blue-800">
              Multiple Tables Support
            </h3>
            <div className="mt-2 text-sm text-blue-700">
              <p>
                Add additional tables to configure joins. Each table will be assigned a unique alias automatically.
                You can modify the alias if needed. The main table is specified above, and additional tables
                will be joined to it in the configuration step.
              </p>
              <p className="mt-2">
                <strong>Note:</strong> Use the "Fill Default Values" button for quick setup, or fill in the fields manually.
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

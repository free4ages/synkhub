import React, { useState } from 'react';
import { Card, CardHeader, CardTitle, CardContent } from '../ui/Card';
import { ExtractedSchema, MigrationConfig, ColumnInfo } from '../../types/configure';
import { configureApi } from '../../services/configureApi';

interface SchemaExtractionStepProps {
  schema: ExtractedSchema | null;
  suggestedConfig: MigrationConfig | null;
  onSuggestedConfigChange?: (config: MigrationConfig) => void;
}

export const SchemaExtractionStep: React.FC<SchemaExtractionStepProps> = ({
  schema,
  suggestedConfig,
  onSuggestedConfigChange
}) => {
  const [selectedColumns, setSelectedColumns] = useState<string[]>([]);
  const [isGeneratingConfig, setIsGeneratingConfig] = useState(false);

  if (!schema) {
    return (
      <div className="text-center py-8">
        <div className="text-gray-500">No schema data available</div>
      </div>
    );
  }

  const handleColumnToggle = (columnName: string) => {
    setSelectedColumns(prev => 
      prev.includes(columnName)
        ? prev.filter(col => col !== columnName)
        : [...prev, columnName]
    );
  };

  const handleSelectAll = () => {
    setSelectedColumns(schema.columns.map(col => col.name));
  };

  const handleDeselectAll = () => {
    setSelectedColumns([]);
  };

  const handleGenerateSuggestedConfig = async () => {
    if (selectedColumns.length === 0) {
      alert('Please select at least one column');
      return;
    }

    setIsGeneratingConfig(true);
    try {
      const config = await configureApi.generateSuggestedConfig(schema, selectedColumns);
      if (onSuggestedConfigChange) {
        onSuggestedConfigChange(config);
      }
    } catch (error) {
      console.error('Failed to generate suggested config:', error);
      alert('Failed to generate suggested configuration');
    } finally {
      setIsGeneratingConfig(false);
    }
  };

  // Group columns by table alias
  const groupColumnsByTable = () => {
    const grouped: { [key: string]: ColumnInfo[] } = {};
    
    schema.columns.forEach(column => {
      const tableAlias = column.table_alias || 'main';
      if (!grouped[tableAlias]) {
        grouped[tableAlias] = [];
      }
      grouped[tableAlias].push(column);
    });
    
    return grouped;
  };

  // Get table name by alias
  const getTableNameByAlias = (alias: string): string => {
    if (alias === 'main') {
      return schema.table_name;
    }
    
    // Find table in the tables list
    const tableInfo = schema.tables?.find(table => table.alias === alias);
    return tableInfo ? tableInfo.table_name : alias;
  };

  const groupedColumns = groupColumnsByTable();

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Extracted Schema</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="mb-4">
            <h3 className="text-lg font-medium text-gray-900 mb-2">
              Database: {schema.database_name || 'Unknown'}
            </h3>
            <div className="text-sm text-gray-600">
              {schema.schema_name && <span>Schema: {schema.schema_name}</span>}
            </div>
          </div>

          {/* Column Selection */}
          <div className="mb-6">
            <div className="flex justify-between items-center mb-4">
              <h4 className="text-lg font-medium">Select Columns for Migration</h4>
              <div className="space-x-2">
                <button
                  type="button"
                  onClick={handleSelectAll}
                  className="px-3 py-1 text-sm bg-blue-600 text-white rounded hover:bg-blue-700"
                >
                  Select All
                </button>
                <button
                  type="button"
                  onClick={handleDeselectAll}
                  className="px-3 py-1 text-sm bg-gray-600 text-white rounded hover:bg-gray-700"
                >
                  Deselect All
                </button>
              </div>
            </div>

            {/* Display columns grouped by table */}
            {Object.entries(groupedColumns).map(([tableAlias, columns]) => (
              <div key={tableAlias} className="mb-6">
                <div className="bg-gray-100 px-4 py-2 rounded-t-md border border-gray-200">
                  <h5 className="text-md font-medium text-gray-900">
                    Table: {getTableNameByAlias(tableAlias)} 
                    {tableAlias !== 'main' && ` (Alias: ${tableAlias})`}
                  </h5>
                </div>
                
                <div className="overflow-x-auto border border-gray-200 rounded-b-md">
                  <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          <input
                            type="checkbox"
                            checked={columns.every(col => selectedColumns.includes(col.name)) && columns.length > 0}
                            onChange={() => {
                              const allSelected = columns.every(col => selectedColumns.includes(col.name));
                              if (allSelected) {
                                setSelectedColumns(prev => prev.filter(col => !columns.find(c => c.name === col)));
                              } else {
                                setSelectedColumns(prev => [...prev, ...columns.map(col => col.name).filter(name => !prev.includes(name))]);
                              }
                            }}
                            className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                          />
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Column Name
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Data Type
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Nullable
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Primary Key
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Unique
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Auto Increment
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Default
                        </th>
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                      {columns.map((column, index) => (
                        <tr key={index} className="hover:bg-gray-50">
                          <td className="px-6 py-4 whitespace-nowrap">
                            <input
                              type="checkbox"
                              checked={selectedColumns.includes(column.name)}
                              onChange={() => handleColumnToggle(column.name)}
                              className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                            />
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                            {column.name}
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                            {column.data_type}
                            {column.max_length && `(${column.max_length})`}
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                            {column.nullable ? 'Yes' : 'No'}
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                            {column.primary_key ? (
                              <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
                                Yes
                              </span>
                            ) : (
                              'No'
                            )}
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                            {column.unique ? (
                              <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
                                Yes
                              </span>
                            ) : (
                              'No'
                            )}
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                            {column.auto_increment ? (
                              <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-purple-100 text-purple-800">
                                Yes
                              </span>
                            ) : (
                              'No'
                            )}
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                            {column.default_value || '-'}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            ))}

            <div className="mt-4 flex justify-between items-center">
              <div className="text-sm text-gray-600">
                {selectedColumns.length} of {schema.columns.length} columns selected
              </div>
              <button
                type="button"
                onClick={handleGenerateSuggestedConfig}
                disabled={selectedColumns.length === 0 || isGeneratingConfig}
                className="px-4 py-2 bg-green-600 text-white rounded-md hover:bg-green-700 disabled:bg-gray-400 disabled:cursor-not-allowed"
              >
                {isGeneratingConfig ? 'Generating...' : 'Generate Suggested Config'}
              </button>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Suggested Configuration */}
      {suggestedConfig && (
        <Card>
          <CardHeader>
            <CardTitle>Suggested Migration Configuration</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              <div>
                <h4 className="text-md font-medium text-gray-900 mb-2">Column Mapping</h4>
                <div className="overflow-x-auto">
                  <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Name
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Source Column (with Alias)
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Destination Column
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Data Type
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Properties
                        </th>
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                      {suggestedConfig.column_map.map((mapping, index) => (
                        <tr key={index} className="hover:bg-gray-50">
                          <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                            {mapping.name}
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                            {mapping.src || 'Enrichment'}
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                            {mapping.dest}
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                            {mapping.dtype || 'varchar'}
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                            <div className="flex flex-wrap gap-1">
                              {mapping.unique_column && (
                                <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
                                  Unique Key
                                </span>
                              )}
                              {mapping.order_column && (
                                <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-green-100 text-green-800">
                                  Order Key
                                </span>
                              )}
                              {mapping.hash_key && (
                                <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-purple-100 text-purple-800">
                                  Hash Key
                                </span>
                              )}
                            </div>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
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
              Schema Extracted Successfully
            </h3>
            <div className="mt-2 text-sm text-green-700">
              <p>
                Select the columns you want to include in the migration and generate a suggested configuration.
                You can review and modify the configuration in the next step.
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

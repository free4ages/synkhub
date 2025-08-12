import React from 'react';
import { Card, CardHeader, CardTitle, CardContent } from '../ui/Card';
import { MigrationConfig, ExtractedSchema, ColumnMapping } from '../../types/configure';

interface ConfigurationStepProps {
  config: MigrationConfig | null;
  onConfigChange: (config: MigrationConfig | null) => void;
  schema: ExtractedSchema | null;
}

export const ConfigurationStep: React.FC<ConfigurationStepProps> = ({
  config,
  onConfigChange,
  schema
}) => {
  if (!config) {
    return (
      <div className="text-center py-8">
        <div className="text-gray-500">No configuration available</div>
      </div>
    );
  }

  const handleConfigChange = (field: keyof MigrationConfig, value: any) => {
    const updatedConfig = {
      ...config,
      [field]: value
    };
    onConfigChange(updatedConfig);
  };

  const handleColumnChange = (index: number, field: keyof ColumnMapping, value: any) => {
    const updatedColumns = [...config.column_map];
    updatedColumns[index] = {
      ...updatedColumns[index],
      [field]: value
    };
    handleConfigChange('column_map', updatedColumns);
  };

  const addColumn = () => {
    const newColumn: ColumnMapping = {
      name: '',
      src: '',
      dest: '',
      dtype: 'text',
      unique_key: false,
      order_key: false,
      hash_key: false,
      insert: true,
      direction: 'asc'
    };
    handleConfigChange('column_map', [...config.column_map, newColumn]);
  };

  const removeColumn = (index: number) => {
    const updatedColumns = config.column_map.filter((_, i) => i !== index);
    handleConfigChange('column_map', updatedColumns);
  };

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Migration Configuration</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Configuration Name
              </label>
              <input
                type="text"
                value={config.name}
                onChange={(e) => handleConfigChange('name', e.target.value)}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Description
              </label>
              <input
                type="text"
                value={config.description}
                onChange={(e) => handleConfigChange('description', e.target.value)}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Partition Key (Optional)
              </label>
              <select
                value={config.partition_key || ''}
                onChange={(e) => handleConfigChange('partition_key', e.target.value || undefined)}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                <option value="">No Partition</option>
                {schema?.columns.map((col) => (
                  <option key={col.name} value={col.name}>
                    {col.name}
                  </option>
                ))}
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Partition Step
              </label>
              <input
                type="number"
                value={config.partition_step || ''}
                onChange={(e) => handleConfigChange('partition_step', parseInt(e.target.value) || undefined)}
                placeholder="1000"
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <div className="flex justify-between items-center">
            <CardTitle>Column Mappings</CardTitle>
            <button
              onClick={addColumn}
              className="px-4 py-2 bg-blue-500 text-white rounded-md hover:bg-blue-600 focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              Add Column
            </button>
          </div>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            {config.column_map.map((column, index) => (
              <div key={index} className="border border-gray-200 rounded-md p-4">
                <div className="flex justify-between items-center mb-4">
                  <h4 className="text-sm font-medium text-gray-900">Column {index + 1}</h4>
                  <button
                    onClick={() => removeColumn(index)}
                    className="text-red-600 hover:text-red-800 text-sm"
                  >
                    Remove
                  </button>
                </div>

                <div className="grid grid-cols-2 gap-4 mb-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Column Name
                    </label>
                    <input
                      type="text"
                      value={column.name}
                      onChange={(e) => handleColumnChange(index, 'name', e.target.value)}
                      className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                    />
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Source Expression
                    </label>
                    <input
                      type="text"
                      value={column.src || ''}
                      onChange={(e) => handleColumnChange(index, 'src', e.target.value)}
                      placeholder="column_name or expression"
                      className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                    />
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Destination Column
                    </label>
                    <input
                      type="text"
                      value={column.dest || ''}
                      onChange={(e) => handleColumnChange(index, 'dest', e.target.value)}
                      placeholder="destination_column_name"
                      className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                    />
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Data Type
                    </label>
                    <select
                      value={column.dtype || ''}
                      onChange={(e) => handleColumnChange(index, 'dtype', e.target.value)}
                      className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                    >
                      <option value="integer">Integer</option>
                      <option value="bigint">BigInt</option>
                      <option value="smallint">SmallInt</option>
                      <option value="float">Float</option>
                      <option value="double">Double</option>
                      <option value="decimal">Decimal</option>
                      <option value="varchar">Varchar</option>
                      <option value="text">Text</option>
                      <option value="char">Char</option>
                      <option value="date">Date</option>
                      <option value="time">Time</option>
                      <option value="timestamp">Timestamp</option>
                      <option value="datetime">DateTime</option>
                      <option value="boolean">Boolean</option>
                      <option value="blob">Blob</option>
                      <option value="json">JSON</option>
                      <option value="uuid">UUID</option>
                    </select>
                  </div>
                </div>

                <div className="grid grid-cols-4 gap-4">
                  <div className="flex items-center">
                    <input
                      type="checkbox"
                      id={`unique-key-${index}`}
                      checked={column.unique_key}
                      onChange={(e) => handleColumnChange(index, 'unique_key', e.target.checked)}
                      className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                    />
                    <label htmlFor={`unique-key-${index}`} className="ml-2 text-sm text-gray-700">
                      Unique Key
                    </label>
                  </div>

                  <div className="flex items-center">
                    <input
                      type="checkbox"
                      id={`order-key-${index}`}
                      checked={column.order_key}
                      onChange={(e) => handleColumnChange(index, 'order_key', e.target.checked)}
                      className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                    />
                    <label htmlFor={`order-key-${index}`} className="ml-2 text-sm text-gray-700">
                      Order Key
                    </label>
                  </div>

                  <div className="flex items-center">
                    <input
                      type="checkbox"
                      id={`hash-key-${index}`}
                      checked={column.hash_key}
                      onChange={(e) => handleColumnChange(index, 'hash_key', e.target.checked)}
                      className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                    />
                    <label htmlFor={`hash-key-${index}`} className="ml-2 text-sm text-gray-700">
                      Hash Key
                    </label>
                  </div>

                  <div className="flex items-center">
                    <input
                      type="checkbox"
                      id={`insert-${index}`}
                      checked={column.insert}
                      onChange={(e) => handleColumnChange(index, 'insert', e.target.checked)}
                      className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                    />
                    <label htmlFor={`insert-${index}`} className="ml-2 text-sm text-gray-700">
                      Insert
                    </label>
                  </div>
                </div>

                {column.order_key && (
                  <div className="mt-4">
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Sort Direction
                    </label>
                    <select
                      value={column.direction || 'asc'}
                      onChange={(e) => handleColumnChange(index, 'direction', e.target.value)}
                      className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                    >
                      <option value="asc">Ascending</option>
                      <option value="desc">Descending</option>
                    </select>
                  </div>
                )}
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      <div className="bg-yellow-50 border border-yellow-200 rounded-md p-4">
        <div className="flex">
          <div className="flex-shrink-0">
            <svg className="h-5 w-5 text-yellow-400" viewBox="0 0 20 20" fill="currentColor">
              <path fillRule="evenodd" d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z" clipRule="evenodd" />
            </svg>
          </div>
          <div className="ml-3">
            <h3 className="text-sm font-medium text-yellow-800">
              Important Notes
            </h3>
            <div className="mt-2 text-sm text-yellow-700">
              <ul className="list-disc list-inside space-y-1">
                <li><strong>Unique Key:</strong> Used for identifying records during sync operations (upserts, conflict resolution)</li>
                <li><strong>Order Key:</strong> Used for sorting data during sync operations</li>
                <li><strong>Hash Key:</strong> Used for calculating row/block hashes for change detection</li>
                <li><strong>Source Expression:</strong> Can be a simple column name or a complex SQL expression</li>
                <li>At least one unique key is required for sync operations</li>
              </ul>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

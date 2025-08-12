import React from 'react';
import { Card, CardHeader, CardTitle, CardContent } from '../ui/Card';
import { ExtractedSchema, MigrationConfig } from '../../types/configure';

interface SchemaExtractionStepProps {
  schema: ExtractedSchema | null;
  suggestedConfig: MigrationConfig | null;
}

export const SchemaExtractionStep: React.FC<SchemaExtractionStepProps> = ({
  schema,
  suggestedConfig
}) => {
  if (!schema) {
    return (
      <div className="text-center py-8">
        <div className="text-gray-500">No schema data available</div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Extracted Schema</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="mb-4">
            <h3 className="text-lg font-medium text-gray-900 mb-2">
              Table: {schema.table_name}
            </h3>
            <div className="text-sm text-gray-600">
              {schema.database_name && <span>Database: {schema.database_name}</span>}
              {schema.schema_name && <span className="ml-4">Schema: {schema.schema_name}</span>}
            </div>
          </div>

          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
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
                {schema.columns.map((column, index) => (
                  <tr key={index} className="hover:bg-gray-50">
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

          {schema.primary_keys.length > 0 && (
            <div className="mt-4 p-4 bg-green-50 border border-green-200 rounded-md">
              <h4 className="text-sm font-medium text-green-800 mb-2">Primary Keys</h4>
              <div className="flex flex-wrap gap-2">
                {schema.primary_keys.map((pk, index) => (
                  <span
                    key={index}
                    className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800"
                  >
                    {pk}
                  </span>
                ))}
              </div>
            </div>
          )}
        </CardContent>
      </Card>

      {suggestedConfig && (
        <Card>
          <CardHeader>
            <CardTitle>Suggested Migration Configuration</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              <div>
                <h4 className="text-sm font-medium text-gray-900 mb-2">Configuration Name</h4>
                <p className="text-sm text-gray-600">{suggestedConfig.name}</p>
              </div>
              
              <div>
                <h4 className="text-sm font-medium text-gray-900 mb-2">Description</h4>
                <p className="text-sm text-gray-600">{suggestedConfig.description}</p>
              </div>

              <div>
                <h4 className="text-sm font-medium text-gray-900 mb-2">Column Mappings</h4>
                <div className="overflow-x-auto">
                  <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Name
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Source
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Destination
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Data Type
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Unique Key
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Order Key
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Hash Key
                        </th>
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                      {suggestedConfig.column_map.map((column, index) => (
                        <tr key={index} className="hover:bg-gray-50">
                          <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                            {column.name}
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                            {column.src || '-'}
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                            {column.dest || '-'}
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                            {column.dtype || '-'}
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                            {column.unique_key ? (
                              <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
                                Yes
                              </span>
                            ) : (
                              'No'
                            )}
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                            {column.order_key ? (
                              <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
                                Yes
                              </span>
                            ) : (
                              'No'
                            )}
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                            {column.hash_key ? (
                              <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-purple-100 text-purple-800">
                                Yes
                              </span>
                            ) : (
                              'No'
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              {suggestedConfig.partition_key && (
                <div>
                  <h4 className="text-sm font-medium text-gray-900 mb-2">Partition Configuration</h4>
                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <span className="text-sm text-gray-500">Partition Key:</span>
                      <span className="ml-2 text-sm font-medium text-gray-900">{suggestedConfig.partition_key}</span>
                    </div>
                    <div>
                      <span className="text-sm text-gray-500">Partition Step:</span>
                      <span className="ml-2 text-sm font-medium text-gray-900">{suggestedConfig.partition_step}</span>
                    </div>
                  </div>
                </div>
              )}
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
                The table schema has been extracted and a suggested migration configuration has been generated. 
                You can review and modify the configuration in the next step.
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

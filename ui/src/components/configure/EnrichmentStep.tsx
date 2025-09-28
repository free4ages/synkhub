import React, { useState, useEffect } from 'react';
import { Card, CardHeader, CardTitle, CardContent } from '../ui/Card';
import { EnrichmentTransformation, ColumnMapping } from '../../types/configure';

interface EnrichmentStepProps {
  transformations: EnrichmentTransformation[];
  onTransformationsChange: (transformations: EnrichmentTransformation[]) => void;
  availableColumns: string[];
  onColumnMappingsUpdate: (mappings: ColumnMapping[]) => void;
}

export const EnrichmentStep: React.FC<EnrichmentStepProps> = ({
  transformations,
  onTransformationsChange,
  availableColumns,
  onColumnMappingsUpdate
}) => {
  const [isAdding, setIsAdding] = useState(false);
  const [newTransformation, setNewTransformation] = useState<EnrichmentTransformation>({
    columns: [],
    transform: '',
    dest: '',
    data_type: 'varchar'
  });

  // Auto-populate hash_key transformation if not already present
  React.useEffect(() => {
    const hasHashKeyTransformation = transformations.some(t => t.dest === 'checksum');
    if (!hasHashKeyTransformation) {
      const hashKeyTransformation: EnrichmentTransformation = {
        columns: ['hash__'],
        transform: 'lambda x: x["hash__"]',
        dest: 'checksum',
        data_type: 'varchar'
      };
      
      const updatedTransformations = [...transformations, hashKeyTransformation];
      onTransformationsChange(updatedTransformations);
      
      // Add to column mappings
      const newColumnMapping: ColumnMapping = {
        name: 'checksum',
        src: undefined, // Enrichment columns have undefined source
        dest: 'checksum',
        data_type: 'varchar',
        unique_column: false,
        order_column: false,
        hash_key: true, // This is the hash key
        insert: true
      };
      
      onColumnMappingsUpdate([newColumnMapping]);
    }
  }, []); // Only run once on mount

  const handleAddTransformation = () => {
    if (newTransformation.columns.length > 0 && newTransformation.transform && newTransformation.dest) {
      const updatedTransformations = [...transformations, newTransformation];
      onTransformationsChange(updatedTransformations);
      
      // Add to column mappings
      const newColumnMapping: ColumnMapping = {
        name: newTransformation.dest,
        src: undefined, // Enrichment columns have undefined source
        dest: newTransformation.dest,
        data_type: newTransformation.data_type,
        unique_column: false,
        order_column: false,
        hash_key: false,
        insert: true
      };
      
      // This will be handled by the parent component
      onColumnMappingsUpdate([newColumnMapping]);
      
      // Reset form
      setNewTransformation({
        columns: [],
        transform: '',
        dest: '',
        data_type: 'varchar'
      });
      setIsAdding(false);
    }
  };

  const handleRemoveTransformation = (index: number) => {
    const updatedTransformations = transformations.filter((_, i) => i !== index);
    onTransformationsChange(updatedTransformations);
  };

  const handleColumnToggle = (columnName: string) => {
    const updatedColumns = newTransformation.columns.includes(columnName)
      ? newTransformation.columns.filter(col => col !== columnName)
      : [...newTransformation.columns, columnName];
    
    setNewTransformation({
      ...newTransformation,
      columns: updatedColumns
    });
  };

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Enrichment Transformations</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <p className="text-sm text-gray-600">
            Define transformations to create computed columns from existing data.
            These columns will be added to your destination table.
          </p>

          {transformations.map((transformation, index) => (
            <div key={index} className="border border-gray-200 rounded-md p-4">
              <div className="flex justify-between items-center mb-3">
                <h4 className="font-medium">Transformation {index + 1}</h4>
                <button
                  type="button"
                  onClick={() => handleRemoveTransformation(index)}
                  className="text-red-600 hover:text-red-800 text-sm"
                >
                  Remove
                </button>
              </div>
              
              <div className="grid grid-cols-2 gap-4 text-sm">
                <div>
                  <span className="font-medium">Source Columns:</span>
                  <div className="mt-1">
                    {transformation.columns.map(col => (
                      <span key={col} className="inline-block bg-blue-100 text-blue-800 px-2 py-1 rounded mr-2 mb-1">
                        {col}
                      </span>
                    ))}
                  </div>
                </div>
                
                <div>
                  <span className="font-medium">Destination:</span>
                  <div className="mt-1 text-gray-700">{transformation.dest}</div>
                </div>
                
                <div>
                  <span className="font-medium">Transform:</span>
                  <div className="mt-1 text-gray-700 font-mono text-xs bg-gray-100 p-2 rounded">
                    {transformation.transform}
                  </div>
                </div>
                
                <div>
                  <span className="font-medium">Data Type:</span>
                  <div className="mt-1 text-gray-700">{transformation.data_type}</div>
                </div>
              </div>
            </div>
          ))}

          {!isAdding ? (
            <button
              type="button"
              onClick={() => setIsAdding(true)}
              className="w-full py-2 px-4 border-2 border-dashed border-gray-300 rounded-md text-gray-600 hover:border-gray-400 hover:text-gray-800 transition-colors"
            >
              + Add Enrichment Transformation
            </button>
          ) : (
            <div className="border border-gray-200 rounded-md p-4 space-y-4">
              <h4 className="font-medium">New Transformation</h4>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Source Columns
                </label>
                <div className="max-h-32 overflow-y-auto border border-gray-300 rounded-md p-2">
                  {availableColumns.map(column => (
                    <label key={column} className="flex items-center space-x-2 py-1">
                      <input
                        type="checkbox"
                        checked={newTransformation.columns.includes(column)}
                        onChange={() => handleColumnToggle(column)}
                        className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                      />
                      <span className="text-sm">{column}</span>
                    </label>
                  ))}
                </div>
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Transform Expression
                </label>
                <textarea
                  value={newTransformation.transform}
                  onChange={(e) => setNewTransformation({
                    ...newTransformation,
                    transform: e.target.value
                  })}
                  placeholder="lambda x: x['column1'] + ' ' + x['column2']"
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                  rows={3}
                />
                <p className="text-xs text-gray-500 mt-1">
                  Use Python lambda expression. Access columns with x['column_name']
                </p>
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Destination Column
                  </label>
                  <input
                    type="text"
                    value={newTransformation.dest}
                    onChange={(e) => setNewTransformation({
                      ...newTransformation,
                      dest: e.target.value
                    })}
                    placeholder="full_name"
                    className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Data Type
                  </label>
                  <select
                    value={newTransformation.data_type}
                    onChange={(e) => setNewTransformation({
                      ...newTransformation,
                      data_type: e.target.value
                    })}
                    className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                  >
                    <option value="varchar">VARCHAR</option>
                    <option value="integer">INTEGER</option>
                    <option value="decimal">DECIMAL</option>
                    <option value="datetime">DATETIME</option>
                    <option value="boolean">BOOLEAN</option>
                    <option value="text">TEXT</option>
                  </select>
                </div>
              </div>

              <div className="flex space-x-2">
                <button
                  type="button"
                  onClick={handleAddTransformation}
                  className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700"
                >
                  Add Transformation
                </button>
                <button
                  type="button"
                  onClick={() => setIsAdding(false)}
                  className="px-4 py-2 border border-gray-300 text-gray-700 rounded-md hover:bg-gray-50"
                >
                  Cancel
                </button>
              </div>
            </div>
          )}
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
              Enrichment Transformations
            </h3>
            <div className="mt-2 text-sm text-green-700">
              <p>
                Create computed columns from existing data using Python lambda expressions.
                These transformations will be applied during the sync process.
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

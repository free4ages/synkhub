import React, { useState } from 'react';
import { Card, CardHeader, CardTitle, CardContent } from '../ui/Card';
import { 
  MigrationConfig, 
  ColumnMapping, 
  FilterCondition, 
  JoinCondition,
  ColumnInfo,
  StrategyConfig
} from '../../types/configure';

interface ConfigurationStepProps {
  config: MigrationConfig | null;
  onConfigChange: (config: MigrationConfig | null) => void;
  schema: any;
  availableColumns: ColumnInfo[];
}

export const ConfigurationStep: React.FC<ConfigurationStepProps> = ({
  config,
  onConfigChange,
  schema,
  availableColumns
}) => {
  const [activeSection, setActiveSection] = useState<'mapping' | 'strategy' | 'source-filters' | 'dest-filters' | 'joins'>('mapping');

  if (!config) return <div>No configuration available</div>;

  // Get available column names for dropdowns
  const getAvailableColumnNames = () => {
    return config.column_map.map(col => col.name).filter(Boolean);
  };

  const handleKeyConfigChange = (field: 'partition_key' | 'partition_step', value: any) => {
    onConfigChange({
      ...config,
      [field]: value
    });
  };

  const handleStrategyChange = (index: number, field: string, value: any) => {
    const updatedStrategies = [...(config.strategies || [])];
    updatedStrategies[index] = { ...updatedStrategies[index], [field]: value };
    
    // Ensure only one strategy of each type is enabled
    if (field === 'enabled' && value === true) {
      const strategyType = updatedStrategies[index].type;
      updatedStrategies.forEach((strategy, i) => {
        if (i !== index && strategy.type === strategyType && strategy.enabled) {
          strategy.enabled = false;
        }
      });
    }
    
    onConfigChange({
      ...config,
      strategies: updatedStrategies
    });
  };

  const handleAddStrategy = () => {
    const availableTypes: ('delta' | 'hash' | 'full')[] = ['delta', 'hash', 'full'];
    const existingTypes = (config.strategies || []).map(s => s.type);
    const newType = availableTypes.find(type => !existingTypes.includes(type));
    
    if (!newType) {
      alert('All strategy types are already added. You can have multiple strategies of the same type, but only one can be enabled.');
      return;
    }
    
    const newStrategy: StrategyConfig = {
      name: newType,
      type: newType,
      enabled: false,
      column: '',
      sub_partition_step: newType === 'delta' ? 30*60 : newType === 'hash' ? 4000 : 5000,
      interval_reduction_factor: 2,
      intervals: [],
      prevent_update_unless_changed: true,
      page_size: newType === 'full' ? 5000 : 1000,
      cron: undefined
    };
    
    const currentStrategies = config.strategies || [];
    onConfigChange({
      ...config,
      strategies: [...currentStrategies, newStrategy]
    });
  };

  const handleRemoveStrategy = (index: number) => {
    const updatedStrategies = (config.strategies || []).filter((_, i) => i !== index);
    onConfigChange({
      ...config,
      strategies: updatedStrategies
    });
  };

  const handleColumnMappingChange = (index: number, field: keyof ColumnMapping, value: any) => {
    const updatedColumnMap = [...config.column_map];
    updatedColumnMap[index] = { ...updatedColumnMap[index], [field]: value };
    
    onConfigChange({
      ...config,
      column_map: updatedColumnMap
    });
  };

  const handleAddColumnMapping = () => {
    const newMapping: ColumnMapping = {
      name: '',
      src: '',
      dest: '',
      dtype: 'varchar',
      unique_key: false,
      order_key: false,
      hash_key: false,
      insert: true
    };
    
    onConfigChange({
      ...config,
      column_map: [...config.column_map, newMapping]
    });
  };

  const handleRemoveColumnMapping = (index: number) => {
    const updatedColumnMap = config.column_map.filter((_, i) => i !== index);
    onConfigChange({
      ...config,
      column_map: updatedColumnMap
    });
  };

  const handleSourceFilterChange = (index: number, field: keyof FilterCondition, value: any) => {
    const updatedFilters = [...(config.source_filters || [])];
    updatedFilters[index] = { ...updatedFilters[index], [field]: value };
    
    onConfigChange({
      ...config,
      source_filters: updatedFilters
    });
  };

  const handleAddSourceFilter = () => {
    const newFilter: FilterCondition = {
      field: '',
      operator: '=',
      value: ''
    };
    
    const currentFilters = config.source_filters || [];
    onConfigChange({
      ...config,
      source_filters: [...currentFilters, newFilter]
    });
  };

  const handleDestFilterChange = (index: number, field: keyof FilterCondition, value: any) => {
    const updatedFilters = [...(config.destination_filters || [])];
    updatedFilters[index] = { ...updatedFilters[index], [field]: value };
    
    onConfigChange({
      ...config,
      destination_filters: updatedFilters
    });
  };

  const handleAddDestFilter = () => {
    const newFilter: FilterCondition = {
      field: '',
      operator: '=',
      value: ''
    };
    
    const currentFilters = config.destination_filters || [];
    onConfigChange({
      ...config,
      destination_filters: [...currentFilters, newFilter]
    });
  };

  const handleJoinChange = (index: number, field: keyof JoinCondition, value: any) => {
    const updatedJoins = [...(config.joins || [])];
    updatedJoins[index] = { ...updatedJoins[index], [field]: value };
    
    onConfigChange({
      ...config,
      joins: updatedJoins
    });
  };

  const handleAddJoin = () => {
    // Get available tables from schema (excluding the main table)
    const availableTables = schema?.tables?.filter((table: any) => table.alias !== 'main') || [];
    
    if (availableTables.length === 0) {
      alert('No additional tables available for joins');
      return;
    }
    
    // Find the first available table that's not already in joins
    const existingJoinTables = (config.joins || []).map(join => join.table);
    const availableTable = availableTables.find((table: any) => !existingJoinTables.includes(table.table_name));
    
    if (!availableTable) {
      alert('All available tables are already added to joins');
      return;
    }
    
    const newJoin: JoinCondition = {
      table: availableTable.table_name,
      alias: availableTable.alias,
      on: `main.id = ${availableTable.alias}.main_id`, // Default join condition
      type: 'LEFT'
    };
    
    const currentJoins = config.joins || [];
    onConfigChange({
      ...config,
      joins: [...currentJoins, newJoin]
    });
  };

  const getDestinationColumns = () => {
    return config.column_map.map(col => col.dest).filter(Boolean);
  };

  const renderColumnMappingSection = () => (
    <div className="space-y-4">
      <div className="flex justify-between items-center">
        <h3 className="text-lg font-medium">Column Mapping</h3>
        <button
          type="button"
          onClick={handleAddColumnMapping}
          className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700"
        >
          Add Column
        </button>
      </div>

      <div className="space-y-4">
        {config.column_map.map((mapping, index) => (
          <div key={index} className="border border-gray-200 rounded-md p-4">
            <div className="flex justify-between items-center mb-4">
              <h4 className="font-medium">Column {index + 1}</h4>
              <button
                type="button"
                onClick={() => handleRemoveColumnMapping(index)}
                className="text-red-600 hover:text-red-800 text-sm"
              >
                Remove
              </button>
            </div>
            
            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Name
                </label>
                <input
                  type="text"
                  value={mapping.name}
                  onChange={(e) => handleColumnMappingChange(index, 'name', e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Source Expression
                </label>
                <input
                  type="text"
                  value={mapping.src || ''}
                  onChange={(e) => handleColumnMappingChange(index, 'src', e.target.value)}
                  placeholder="u.id or p.email"
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Destination Column
                </label>
                <input
                  type="text"
                  value={mapping.dest || ''}
                  onChange={(e) => handleColumnMappingChange(index, 'dest', e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Data Type
                </label>
                <select
                  value={mapping.dtype || 'varchar'}
                  onChange={(e) => handleColumnMappingChange(index, 'dtype', e.target.value)}
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
            
            <div className="mt-4 grid grid-cols-4 gap-4">
              <label className="flex items-center">
                <input
                  type="checkbox"
                  checked={mapping.unique_key}
                  onChange={(e) => handleColumnMappingChange(index, 'unique_key', e.target.checked)}
                  className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                />
                <span className="ml-2 text-sm">Unique Key</span>
              </label>
              
              <label className="flex items-center">
                <input
                  type="checkbox"
                  checked={mapping.order_key}
                  onChange={(e) => handleColumnMappingChange(index, 'order_key', e.target.checked)}
                  className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                />
                <span className="ml-2 text-sm">Order Key</span>
              </label>
              
              <label className="flex items-center">
                <input
                  type="checkbox"
                  checked={mapping.hash_key}
                  onChange={(e) => handleColumnMappingChange(index, 'hash_key', e.target.checked)}
                  className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                />
                <span className="ml-2 text-sm">Hash Key</span>
              </label>
              
              <label className="flex items-center">
                <input
                  type="checkbox"
                  checked={mapping.insert}
                  onChange={(e) => handleColumnMappingChange(index, 'insert', e.target.checked)}
                  className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                />
                <span className="ml-2 text-sm">Insert</span>
              </label>
            </div>
          </div>
        ))}
      </div>
    </div>
  );

  const renderSourceFiltersSection = () => (
    <div className="space-y-4">
      <div className="flex justify-between items-center">
        <h3 className="text-lg font-medium">Source Filter Configuration (Optional)</h3>
        <button
          type="button"
          onClick={handleAddSourceFilter}
          className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700"
        >
          Add Filter
        </button>
      </div>

      <div className="space-y-4">
        {(config.source_filters || []).map((filter, index) => (
          <div key={index} className="border border-gray-200 rounded-md p-4">
            <div className="grid grid-cols-4 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Field
                </label>
                <select
                  value={filter.field}
                  onChange={(e) => handleSourceFilterChange(index, 'field', e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                >
                  <option value="">Select field</option>
                  {availableColumns.map(col => (
                    <option key={col.name} value={col.name}>
                      {col.table_alias ? `${col.table_alias}.${col.name}` : col.name}
                    </option>
                  ))}
                </select>
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Operator
                </label>
                <select
                  value={filter.operator}
                  onChange={(e) => handleSourceFilterChange(index, 'operator', e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                >
                  <option value="=">=</option>
                  <option value="!=">!=</option>
                  <option value="&gt;">&gt;</option>
                  <option value="&lt;">&lt;</option>
                  <option value="&gt;=">&gt;=</option>
                  <option value="&lt;=">&lt;=</option>
                  <option value="LIKE">LIKE</option>
                  <option value="IN">IN</option>
                  <option value="NOT IN">NOT IN</option>
                  <option value="IS NULL">IS NULL</option>
                  <option value="IS NOT NULL">IS NOT NULL</option>
                </select>
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Value
                </label>
                <input
                  type="text"
                  value={String(filter.value || '')}
                  onChange={(e) => handleSourceFilterChange(index, 'value', e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
              
              <div className="flex items-end">
                <button
                  type="button"
                  onClick={() => {
                    const updatedFilters = config.source_filters?.filter((_, i) => i !== index) || [];
                    onConfigChange({
                      ...config,
                      source_filters: updatedFilters
                    });
                  }}
                  className="px-3 py-2 text-red-600 hover:text-red-800 text-sm"
                >
                  Remove
                </button>
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );

  const renderDestFiltersSection = () => (
    <div className="space-y-4">
      <div className="flex justify-between items-center">
        <h3 className="text-lg font-medium">Destination Filter Configuration (Optional)</h3>
        <button
          type="button"
          onClick={handleAddDestFilter}
          className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700"
        >
          Add Filter
        </button>
      </div>

      <div className="space-y-4">
        {(config.destination_filters || []).map((filter, index) => (
          <div key={index} className="border border-gray-200 rounded-md p-4">
            <div className="grid grid-cols-4 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Field
                </label>
                <select
                  value={filter.field}
                  onChange={(e) => handleDestFilterChange(index, 'field', e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                >
                  <option value="">Select field</option>
                  {getDestinationColumns().map(col => (
                    <option key={col} value={col}>{col}</option>
                  ))}
                </select>
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Operator
                </label>
                <select
                  value={filter.operator}
                  onChange={(e) => handleDestFilterChange(index, 'operator', e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                >
                  <option value="=">=</option>
                  <option value="!=">!=</option>
                  <option value="&gt;">&gt;</option>
                  <option value="&lt;">&lt;</option>
                  <option value="&gt;=">&gt;=</option>
                  <option value="&lt;=">&lt;=</option>
                  <option value="LIKE">LIKE</option>
                  <option value="IN">IN</option>
                  <option value="NOT IN">NOT IN</option>
                  <option value="IS NULL">IS NULL</option>
                  <option value="IS NOT NULL">IS NOT NULL</option>
                </select>
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Value
                </label>
                <input
                  type="text"
                  value={String(filter.value || '')}
                  onChange={(e) => handleDestFilterChange(index, 'value', e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
              
              <div className="flex items-end">
                <button
                  type="button"
                  onClick={() => {
                    const updatedFilters = config.destination_filters?.filter((_, i) => i !== index) || [];
                    onConfigChange({
                      ...config,
                      destination_filters: updatedFilters
                    });
                  }}
                  className="px-3 py-2 text-red-600 hover:text-red-800 text-sm"
                >
                  Remove
                </button>
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );

  const renderJoinsSection = () => (
    <div className="space-y-4">
      <div className="flex justify-between items-center">
        <h3 className="text-lg font-medium">Join Conditions</h3>
        <button
          type="button"
          onClick={handleAddJoin}
          className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700"
        >
          Add Join
        </button>
      </div>

      <div className="bg-blue-50 border border-blue-200 rounded-md p-4 mb-4">
        <div className="flex">
          <div className="flex-shrink-0">
            <svg className="h-5 w-5 text-blue-400" viewBox="0 0 20 20" fill="currentColor">
              <path fillRule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clipRule="evenodd" />
            </svg>
          </div>
          <div className="ml-3">
            <h3 className="text-sm font-medium text-blue-800">
              Join Configuration
            </h3>
            <div className="mt-2 text-sm text-blue-700">
              <p>Tables and aliases are automatically populated from your selected tables. You only need to specify the join type and condition.</p>
            </div>
          </div>
        </div>
      </div>

      <div className="space-y-4">
        {(config.joins || []).map((join, index) => (
          <div key={index} className="border border-gray-200 rounded-md p-4">
            <div className="grid grid-cols-4 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Table
                </label>
                <input
                  type="text"
                  value={join.table}
                  readOnly
                  className="w-full px-3 py-2 border border-gray-300 rounded-md bg-gray-50 text-gray-600 cursor-not-allowed"
                />
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Alias
                </label>
                <input
                  type="text"
                  value={join.alias}
                  readOnly
                  className="w-full px-3 py-2 border border-gray-300 rounded-md bg-gray-50 text-gray-600 cursor-not-allowed"
                />
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Join Type
                </label>
                <select
                  value={join.type}
                  onChange={(e) => handleJoinChange(index, 'type', e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                >
                  <option value="INNER">INNER</option>
                  <option value="LEFT">LEFT</option>
                  <option value="RIGHT">RIGHT</option>
                  <option value="FULL">FULL</option>
                </select>
              </div>
              
              <div className="flex items-end">
                <button
                  type="button"
                  onClick={() => {
                    const updatedJoins = config.joins?.filter((_, i) => i !== index) || [];
                    onConfigChange({
                      ...config,
                      joins: updatedJoins
                    });
                  }}
                  className="px-3 py-2 text-red-600 hover:text-red-800 text-sm"
                >
                  Remove
                </button>
              </div>
            </div>
            
            <div className="mt-4">
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Join Condition
              </label>
              <input
                type="text"
                value={join.on}
                onChange={(e) => handleJoinChange(index, 'on', e.target.value)}
                placeholder="main.id = p.user_id"
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
              <p className="text-xs text-gray-500 mt-1">
                SQL join condition (e.g., "main.id = p.user_id")
              </p>
            </div>
          </div>
        ))}
      </div>
    </div>
  );

  const renderStrategySection = () => (
    <div className="space-y-6">
      {/* Section 1: Key Configuration */}
      <div className="space-y-4">
        <h3 className="text-lg font-medium">Key Configuration</h3>
        
        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Partition Key <span className="text-red-500">*</span>
            </label>
            <select
              value={config.partition_key || ''}
              onChange={(e) => handleKeyConfigChange('partition_key', e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              required
            >
              <option value="">Select partition key</option>
              {getAvailableColumnNames().map(col => (
                <option key={col} value={col}>{col}</option>
              ))}
            </select>
            <p className="text-xs text-gray-500 mt-1">
              Column used for partitioning data during sync
            </p>
          </div>
          
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Partition Step <span className="text-red-500">*</span>
            </label>
            <input
              type="number"
              value={config.partition_step || ''}
              onChange={(e) => handleKeyConfigChange('partition_step', parseInt(e.target.value) || 0)}
              placeholder="1000"
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              required
            />
            <p className="text-xs text-gray-500 mt-1">
              Number of records per partition
            </p>
          </div>
        </div>
      </div>

      {/* Section 2: Strategy Configuration */}
      <div className="space-y-4">
        <h3 className="text-lg font-medium">Strategy Configuration</h3>
        
        <div className="space-y-4">
          {(config.strategies || []).map((strategy, index) => (
            <div key={index} className="border border-gray-200 rounded-md p-4">
              <div className="flex justify-between items-center mb-4">
                <h4 className="font-medium">{strategy.name} Strategy</h4>
                <div className="flex items-center space-x-2">
                  <label className="flex items-center">
                    <input
                      type="checkbox"
                      checked={strategy.enabled}
                      onChange={(e) => handleStrategyChange(index, 'enabled', e.target.checked)}
                      className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                    />
                    <span className="ml-2 text-sm">Enabled</span>
                  </label>
                  <button
                    type="button"
                    onClick={() => handleRemoveStrategy(index)}
                    className="px-3 py-2 text-red-600 hover:text-red-800 text-sm rounded-md"
                  >
                    Remove Strategy
                  </button>
                </div>
              </div>
              
              <div className="grid grid-cols-3 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Type
                  </label>
                  <input
                    type="text"
                    value={strategy.type}
                    readOnly
                    className="w-full px-3 py-2 border border-gray-300 rounded-md bg-gray-50 text-gray-600"
                  />
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Column
                  </label>
                  <select
                    value={strategy.column || ''}
                    onChange={(e) => handleStrategyChange(index, 'column', e.target.value)}
                    className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                  >
                    <option value="">Select column</option>
                    {getAvailableColumnNames().map(col => (
                      <option key={col} value={col}>{col}</option>
                    ))}
                  </select>
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Sub Partition Step
                  </label>
                  <input
                    type="number"
                    value={strategy.sub_partition_step || ''}
                    onChange={(e) => handleStrategyChange(index, 'sub_partition_step', parseInt(e.target.value) || 0)}
                    className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                  />
                </div>
              </div>
              
              {/* Page Size for all strategies */}
              <div className="mt-4">
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Page Size
                </label>
                <input
                  type="number"
                  value={strategy.page_size || ''}
                  onChange={(e) => handleStrategyChange(index, 'page_size', parseInt(e.target.value) || 0)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
                <p className="text-xs text-gray-500 mt-1">
                  Number of records per page for data processing
                </p>
              </div>
              
              {/* Hash Strategy Specific Fields */}
              {strategy.type === 'hash' && (
                <div className="mt-4 space-y-4">
                  <div className="p-3 bg-blue-50 border border-blue-200 rounded-md">
                    <p className="text-sm text-blue-800 font-medium mb-2">Hash Strategy Specific Fields:</p>
                    <p className="text-xs text-blue-700">The following fields are only applicable to hash strategy</p>
                  </div>
                  
                  <div className="grid grid-cols-3 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">
                        Interval Reduction Factor
                      </label>
                      <input
                        type="number"
                        value={strategy.interval_reduction_factor || ''}
                        onChange={(e) => handleStrategyChange(index, 'interval_reduction_factor', parseInt(e.target.value) || 0)}
                        placeholder="2"
                        className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                      />
                      <p className="text-xs text-gray-500 mt-1">
                        Factor to reduce intervals
                      </p>
                    </div>
                    
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">
                        Intervals
                      </label>
                      <input
                        type="text"
                        value={strategy.intervals?.join(', ') || ''}
                        onChange={(e) => {
                          const intervals = e.target.value.split(',').map(s => parseInt(s.trim())).filter(n => !isNaN(n));
                          handleStrategyChange(index, 'intervals', intervals);
                        }}
                        placeholder="100, 200, 500"
                        className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                      />
                      <p className="text-xs text-gray-500 mt-1">
                        Comma-separated interval values
                      </p>
                    </div>
                    
                    <div className="flex items-center">
                      <label className="flex items-center">
                        <input
                          type="checkbox"
                          checked={strategy.prevent_update_unless_changed || false}
                          onChange={(e) => handleStrategyChange(index, 'prevent_update_unless_changed', e.target.checked)}
                          className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                        />
                        <span className="ml-2 text-sm">Prevent Update Unless Changed</span>
                      </label>
                    </div>
                  </div>
                </div>
              )}
              
              {/* Cron Expression for all strategies */}
              <div className="mt-4">
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Cron Expression (Optional)
                </label>
                <input
                  type="text"
                  value={strategy.cron || ''}
                  onChange={(e) => handleStrategyChange(index, 'cron', e.target.value)}
                  placeholder="0 0 * * *"
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
                <p className="text-xs text-gray-500 mt-1">
                  Cron expression for scheduling (e.g., "0 0 * * *" for daily at midnight)
                </p>
              </div>
            </div>
          ))}
          <button
            type="button"
            onClick={handleAddStrategy}
            className="px-4 py-2 bg-green-600 text-white rounded-md hover:bg-green-700"
          >
            Add New Strategy
          </button>
        </div>
      </div>
    </div>
  );

  return (
    <div className="space-y-6">
      {/* Section Navigation */}
      <Card>
        <CardHeader>
          <CardTitle>Migration Configuration</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex space-x-2 mb-6">
            <button
              onClick={() => setActiveSection('mapping')}
              className={`px-4 py-2 rounded-md ${
                activeSection === 'mapping'
                  ? 'bg-blue-600 text-white'
                  : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
              }`}
            >
              Column Mapping
            </button>
            <button
              onClick={() => setActiveSection('strategy')}
              className={`px-4 py-2 rounded-md ${
                activeSection === 'strategy'
                  ? 'bg-blue-600 text-white'
                  : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
              }`}
            >
              Strategy Configuration
            </button>
            <button
              onClick={() => setActiveSection('source-filters')}
              className={`px-4 py-2 rounded-md ${
                activeSection === 'source-filters'
                  ? 'bg-blue-600 text-white'
                  : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
              }`}
            >
              Source Filters
            </button>
            <button
              onClick={() => setActiveSection('dest-filters')}
              className={`px-4 py-2 rounded-md ${
                activeSection === 'dest-filters'
                  ? 'bg-blue-600 text-white'
                  : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
              }`}
            >
              Destination Filters
            </button>
            <button
              onClick={() => setActiveSection('joins')}
              className={`px-4 py-2 rounded-md ${
                activeSection === 'joins'
                  ? 'bg-blue-600 text-white'
                  : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
              }`}
            >
              Join Conditions
            </button>
          </div>

          {/* Section Content */}
          <div className="mt-6">
            {activeSection === 'mapping' && renderColumnMappingSection()}
            {activeSection === 'strategy' && renderStrategySection()}
            {activeSection === 'source-filters' && renderSourceFiltersSection()}
            {activeSection === 'dest-filters' && renderDestFiltersSection()}
            {activeSection === 'joins' && renderJoinsSection()}
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
              Configuration Notes
            </h3>
            <div className="mt-2 text-sm text-yellow-700">
              <ul className="list-disc list-inside space-y-1">
                <li><strong>Column Mapping:</strong> Map source columns to destination columns with sync properties</li>
                <li><strong>Strategy Configuration:</strong> Configure partition key, partition step, and sync strategies</li>
                <li><strong>Source Filters:</strong> Apply filters to source data before sync (supports joined tables)</li>
                <li><strong>Destination Filters:</strong> Apply filters to destination data (only mapped columns available)</li>
                <li><strong>Join Conditions:</strong> Tables and aliases are automatically populated - you only need to specify join type and condition</li>
                <li>At least one unique key is required for sync operations</li>
                <li>Hash strategy includes additional fields: interval reduction factor, intervals, and prevent update unless changed</li>
                <li>Only one strategy of each type can be enabled at a time</li>
              </ul>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

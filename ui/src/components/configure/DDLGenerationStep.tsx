import React, { useState, useEffect } from 'react';
import { Card, CardHeader, CardTitle, CardContent } from '../ui/Card';
import { MigrationConfig } from '../../types/configure';

interface DDLGenerationStepProps {
  config: MigrationConfig | null;
  onConfigChange: (config: MigrationConfig | null) => void;
  destinationConnection: any;
  destinationTableName: string;
  destinationSchema: string;
  enrichmentTransformations: any[];
  generatedDDL: string;
  configYaml: string;
  configJson: string;
}

export const DDLGenerationStep: React.FC<DDLGenerationStepProps> = ({
  config,
  onConfigChange,
  destinationConnection,
  destinationTableName,
  destinationSchema,
  enrichmentTransformations,
  generatedDDL,
  configYaml,
  configJson
}) => {
  const [activeTab, setActiveTab] = useState<'ddl' | 'yaml' | 'json'>('ddl');
  const [editorContent, setEditorContent] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [isValid, setIsValid] = useState(false);
  const [mode, setMode] = useState<'yaml' | 'json'>('yaml');

  // Convert config to YAML or JSON string
  const configToString = (config: MigrationConfig, format: 'yaml' | 'json'): string => {
    try {
      if (format === 'yaml') {
        // Merge enrichment transformations with column_map
        const mergedColumnMap = [...config.column_map];
        
        if (config.enrichment && config.enrichment.enabled && config.enrichment.transformations.length > 0) {
          config.enrichment.transformations.forEach(transform => {
            // Check if enrichment column already exists in column_map
            const existingColumn = mergedColumnMap.find(col => col.name === transform.dest);
            if (!existingColumn) {
              // Add enrichment column to column_map with src as null
              mergedColumnMap.push({
                name: transform.dest,
                src: null, // Enrichment fields have src as null
                dest: transform.dest,
                dtype: transform.dtype,
                unique_column: false,
                order_column: false,
                hash_key: false,
                insert: true,
                direction: null
              });
            }
          });
        }
        
        // Simple YAML conversion (you might want to use a proper YAML library)
        let yaml = `name: ${config.name}
description: ${config.description}
max_concurrent_partitions: 4
source_provider:
  data_backend:
    type: ${config.source_provider.data_backend.type}
    connection:
      host: ${config.source_provider.data_backend.connection.host}
      port: ${config.source_provider.data_backend.connection.port}
      database: ${config.source_provider.data_backend.connection.database}
      user: ${config.source_provider.data_backend.connection.user}
      password: ${config.source_provider.data_backend.connection.password}
      schema: ${config.source_provider.data_backend.connection.schema || 'null'}
    table: ${config.source_provider.data_backend.table}
    alias: ${config.source_provider.data_backend.alias || 'null'}
    schema: ${config.source_provider.data_backend.connection.schema || 'null'}`;

        // Add filters to source provider if present
        console.log('Processing source_filters:', config.source_filters);
        console.log('Processing destination_filters:', config.destination_filters);
        if (config.source_filters && config.source_filters.length > 0) {
          yaml += `\n    filters:`;
          config.source_filters.forEach(filter => {
            yaml += `\n      - field: ${filter.field}
        operator: ${filter.operator}
        value: ${filter.value !== undefined ? filter.value : 'null'}
        table_alias: ${filter.table_alias || 'null'}`;
          });
        } else {
          yaml += `\n    filters: []`;
        }

        // Add joins to source provider if present
        console.log('Processing joins:', config.joins);
        if (config.joins && config.joins.length > 0) {
          yaml += `\n    join:`;
          config.joins.forEach(join => {
            yaml += `\n      - table: ${join.table}
        alias: ${join.alias}
        on: ${join.on}
        type: ${join.type}`;
          });
        }

        yaml += `\ndestination_provider:
  data_backend:
    type: ${config.destination_provider.data_backend.type}
    connection:
      host: ${config.destination_provider.data_backend.connection.host}
      port: ${config.destination_provider.data_backend.connection.port}
      database: ${config.destination_provider.data_backend.connection.database}
      user: ${config.destination_provider.data_backend.connection.user}
      password: ${config.destination_provider.data_backend.connection.password}
      schema: ${config.destination_provider.data_backend.connection.schema || 'null'}
    table: ${config.destination_provider.data_backend.table}
    schema: ${config.destination_provider.data_backend.connection.schema || 'null'}
    supports_update: true`;

        // Add destination filters if present
        if (config.destination_filters && config.destination_filters.length > 0) {
          yaml += `\n    filters:`;
          config.destination_filters.forEach(filter => {
            yaml += `\n      - field: ${filter.field}
        operator: ${filter.operator}
        value: ${filter.value !== undefined ? filter.value : 'null'}
        table_alias: ${filter.table_alias || 'null'}`;
          });
        } else {
          yaml += `\n    filters: []`;
        }

        yaml += `\ncolumn_map:
${mergedColumnMap.map(col => `  - name: ${col.name}
    src: ${col.src || 'null'}
    dest: ${col.dest || 'null'}
    dtype: ${col.dtype || 'null'}
    unique_column: ${col.unique_column}
    order_column: ${col.order_column}
    hash_key: ${col.hash_key}
    insert: ${col.insert}
    direction: ${col.direction || 'null'}`).join('\n')}
partition_column: ${config.partition_column || 'null'}
partition_step: ${config.partition_step || 'null'}`;



        // Add strategies if present
        if (config.strategies && config.strategies.length > 0) {
          yaml += `\nstrategies:`;
          config.strategies.forEach(strategy => {
            yaml += `\n  - name: ${strategy.name}
    type: ${strategy.type}
    enabled: ${strategy.enabled}
    column: ${strategy.column || 'null'}
    sub_partition_step: ${strategy.sub_partition_step || 'null'}
    interval_reduction_factor: ${strategy.interval_reduction_factor || 'null'}
    prevent_update_unless_changed: ${strategy.prevent_update_unless_changed}
    page_size: ${strategy.page_size || 'null'}
    cron: ${strategy.cron || 'null'}`;
          });
        }

        // Add enrichment if present
        if (config.enrichment && config.enrichment.enabled && config.enrichment.transformations.length > 0) {
          console.log('Adding enrichment to config:', config.enrichment);
          yaml += `\nenrichment:
  enabled: ${config.enrichment.enabled}
  transformations:`;
          config.enrichment.transformations.forEach(transform => {
            yaml += `\n    - columns:
        - ${transform.columns.join('\n        - ')}
      transform: ${transform.transform}
      dest: ${transform.dest}
      dtype: ${transform.dtype}`;
          });
        } else {
          console.log('No enrichment data found:', config.enrichment);
        }

        return yaml;
      } else {
        return JSON.stringify(config, null, 2);
      }
    } catch (error) {
      console.error('Error converting config to string:', error);
      return '';
    }
  };

  // Parse string back to config object
  const stringToConfig = (content: string, format: 'yaml' | 'json'): MigrationConfig | null => {
    try {
      if (format === 'yaml') {
        // Simple YAML parsing (you might want to use a proper YAML library)
        const lines = content.split('\n');
        const config: any = {};
        let currentSection = '';
        let currentArray = '';
        
        for (const line of lines) {
          const trimmed = line.trim();
          if (!trimmed || trimmed.startsWith('#')) continue;
          
          if (trimmed.includes(':')) {
            const [key, ...valueParts] = trimmed.split(':');
            const value = valueParts.join(':').trim();
            
            if (key === 'name' || key === 'description') {
              config[key] = value;
            } else if (key === 'source_provider' || key === 'destination_provider') {
              currentSection = key;
              config[key] = { data_backend: { connection: {} } };
            } else if (key === 'column_map') {
              currentArray = key;
              config[key] = [];
            } else if (key === 'source_filters') {
              currentArray = key;
              config[key] = [];
            } else if (key === 'destination_filters') {
              currentArray = key;
              config[key] = [];
            } else if (key === 'joins') {
              currentArray = key;
              config[key] = [];
            } else if (key === 'strategies') {
              currentArray = key;
              config[key] = [];
            } else if (key === 'enrichment') {
              currentSection = key;
              config[key] = { enabled: false, transformations: [] };
            } else if (key === 'partition_column' || key === 'partition_step') {
              config[key] = value === 'null' ? null : (key === 'partition_step' ? parseInt(value) : value);
            } else if (currentSection && key.includes('.')) {
              const [section, subKey] = key.split('.');
              if (section === 'connection') {
                config[currentSection].data_backend.connection[subKey] = 
                  value === 'null' ? null : (subKey === 'port' ? parseInt(value) : value);
              } else {
                config[currentSection].data_backend[subKey] = 
                  value === 'null' ? null : (subKey === 'port' ? parseInt(value) : value);
              }
            } else if (currentArray === 'column_map' && key.startsWith('-')) {
              // Handle array items
              const column: any = {};
              const columnLines = lines.slice(lines.indexOf(line));
              for (const colLine of columnLines) {
                const colTrimmed = colLine.trim();
                if (colTrimmed.startsWith('-') || colTrimmed === '') continue;
                if (colTrimmed.includes(':')) {
                  const [colKey, ...colValueParts] = colTrimmed.split(':');
                  const colValue = colValueParts.join(':').trim();
                  column[colKey.trim()] = 
                    colValue === 'null' ? null : 
                    colValue === 'true' ? true : 
                    colValue === 'false' ? false : colValue;
                }
              }
              if (Object.keys(column).length > 0) {
                config.column_map.push(column);
              }
            } else if (currentArray === 'strategies' && key.startsWith('-')) {
              // Handle strategies array items
              const strategy: any = {};
              const strategyLines = lines.slice(lines.indexOf(line));
              for (const stratLine of strategyLines) {
                const stratTrimmed = stratLine.trim();
                if (stratTrimmed.startsWith('-') || stratTrimmed === '') continue;
                if (stratTrimmed.includes(':')) {
                  const [stratKey, ...stratValueParts] = stratTrimmed.split(':');
                  const stratValue = stratValueParts.join(':').trim();
                  strategy[stratKey.trim()] = 
                    stratValue === 'null' ? null : 
                    stratValue === 'true' ? true : 
                    stratValue === 'false' ? false : stratValue;
                }
              }
              if (Object.keys(strategy).length > 0) {
                config.strategies.push(strategy);
              }
            } else if (currentArray === 'source_filters' && key.startsWith('-')) {
              // Handle source filters array items
              const filter: any = {};
              const filterLines = lines.slice(lines.indexOf(line));
              for (const filtLine of filterLines) {
                const filtTrimmed = filtLine.trim();
                if (filtTrimmed.startsWith('-') || filtTrimmed === '') continue;
                if (filtTrimmed.includes(':')) {
                  const [filtKey, ...filtValueParts] = filtTrimmed.split(':');
                  const filtValue = filtValueParts.join(':').trim();
                  filter[filtKey.trim()] = 
                    filtValue === 'null' ? null : 
                    filtValue === 'true' ? true : 
                    filtValue === 'false' ? false : filtValue;
                }
              }
              if (Object.keys(filter).length > 0) {
                config.source_filters.push(filter);
              }
            } else if (currentArray === 'destination_filters' && key.startsWith('-')) {
              // Handle destination filters array items
              const filter: any = {};
              const filterLines = lines.slice(lines.indexOf(line));
              for (const filtLine of filterLines) {
                const filtTrimmed = filtLine.trim();
                if (filtTrimmed.startsWith('-') || filtTrimmed === '') continue;
                if (filtTrimmed.includes(':')) {
                  const [filtKey, ...filtValueParts] = filtTrimmed.split(':');
                  const filtValue = filtValueParts.join(':').trim();
                  filter[filtKey.trim()] = 
                    filtValue === 'null' ? null : 
                    filtValue === 'true' ? true : 
                    filtValue === 'false' ? false : filtValue;
                }
              }
              if (Object.keys(filter).length > 0) {
                config.destination_filters.push(filter);
              }
            } else if (currentArray === 'joins' && key.startsWith('-')) {
              // Handle joins array items
              const join: any = {};
              const joinLines = lines.slice(lines.indexOf(line));
              for (const joinLine of joinLines) {
                const joinTrimmed = joinLine.trim();
                if (joinTrimmed.startsWith('-') || joinTrimmed === '') continue;
                if (joinTrimmed.includes(':')) {
                  const [joinKey, ...joinValueParts] = joinLine.split(':');
                  const joinValue = joinValueParts.join(':').trim();
                  join[joinKey.trim()] = joinValue;
                }
              }
              if (Object.keys(join).length > 0) {
                config.joins.push(join);
              }
            } else if (currentSection === 'enrichment' && key === 'enabled') {
              config.enrichment.enabled = value === 'true';
            } else if (currentSection === 'enrichment' && key === 'transformations') {
              currentArray = 'enrichment_transformations';
              config.enrichment.transformations = [];
            } else if (currentArray === 'enrichment_transformations' && key.startsWith('-')) {
              // Handle enrichment transformations array items
              const transform: any = {};
              const transformLines = lines.slice(lines.indexOf(line));
              for (const transLine of transformLines) {
                const transTrimmed = transLine.trim();
                if (transTrimmed.startsWith('-') || transTrimmed === '') continue;
                if (transTrimmed.includes(':')) {
                  const [transKey, ...transValueParts] = transTrimmed.split(':');
                  const transValue = transValueParts.join(':').trim();
                  if (transKey.trim() === 'columns') {
                    // Parse columns array
                    const columnsMatch = transValue.match(/\[(.*)\]/);
                    if (columnsMatch) {
                      transform.columns = columnsMatch[1].split(',').map(col => col.trim());
                    }
                  } else {
                    transform[transKey.trim()] = transValue;
                  }
                }
              }
              if (Object.keys(transform).length > 0) {
                config.enrichment.transformations.push(transform);
              }
            }
          }
        }
        
        return config as MigrationConfig;
      } else {
        return JSON.parse(content);
      }
    } catch (error) {
      console.error('Error parsing config:', error);
      return null;
    }
  };

  // Initialize editor content when config changes
  useEffect(() => {
    if (config) {
      console.log('DDLGenerationStep: enrichmentTransformations prop:', enrichmentTransformations);
      console.log('DDLGenerationStep: original config:', config);
      
      // Update destination provider with collected information
      const updatedConfig = {
        ...config,
        destination_provider: {
          data_backend: {
            type: destinationConnection?.type || 'postgres',
            connection: {
              host: destinationConnection?.host || 'localhost',
              port: destinationConnection?.port || 5432,
              database: destinationConnection?.database || '',
              user: destinationConnection?.user || '',
              password: destinationConnection?.password || '',
              schema: destinationSchema || destinationConnection?.db_schema || null
            },
            table: destinationTableName || config.destination_provider.data_backend.table
          }
        },
        // Ensure enrichment data is preserved
        enrichment: {
          enabled: enrichmentTransformations.length > 0,
          transformations: enrichmentTransformations
        },
        // Ensure filters and joins are preserved and initialized
        source_filters: config.source_filters || [],
        destination_filters: config.destination_filters || [],
        joins: config.joins || []
      };

      console.log('DDLGenerationStep: updatedConfig.enrichment:', updatedConfig.enrichment);

      const content = configToString(updatedConfig, mode);
      setEditorContent(content);
      setError(null);
      setIsValid(true);
      // Initialize editedConfig with the updated config
      onConfigChange(updatedConfig);
    }
  }, [config, mode, destinationConnection, destinationTableName, destinationSchema, enrichmentTransformations]);

  // Handle editor content changes
  const handleEditorChange = (content: string) => {
    setEditorContent(content);
    
    try {
      const parsedConfig = stringToConfig(content, mode);
      if (parsedConfig) {
        onConfigChange(parsedConfig);
        setError(null);
        setIsValid(true);
      } else {
        setError('Invalid configuration format');
        setIsValid(false);
      }
    } catch (error) {
      setError(`Parse error: ${error instanceof Error ? error.message : 'Unknown error'}`);
      setIsValid(false);
    }
  };

  // Handle format change
  const handleFormatChange = (newMode: 'yaml' | 'json') => {
    if (config) {
      const content = configToString(config, newMode);
      setEditorContent(content);
      setMode(newMode);
      setError(null);
      setIsValid(true);
    }
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
          <div className="flex justify-between items-center">
            <CardTitle>Edit Configuration</CardTitle>
            <div className="flex space-x-2">
              <button
                onClick={() => handleFormatChange('yaml')}
                className={`px-3 py-1 rounded text-sm font-medium transition-colors ${
                  mode === 'yaml'
                    ? 'bg-blue-500 text-white'
                    : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                }`}
              >
                YAML
              </button>
              <button
                onClick={() => handleFormatChange('json')}
                className={`px-3 py-1 rounded text-sm font-medium transition-colors ${
                  mode === 'json'
                    ? 'bg-blue-500 text-white'
                    : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                }`}
              >
                JSON
              </button>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            {error && (
              <div className="bg-red-50 border border-red-200 rounded-md p-4">
                <div className="flex">
                  <div className="flex-shrink-0">
                    <svg className="h-5 w-5 text-red-400" viewBox="0 0 20 20" fill="currentColor">
                      <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clipRule="evenodd" />
                    </svg>
                  </div>
                  <div className="ml-3">
                    <h3 className="text-sm font-medium text-red-800">Configuration Error</h3>
                    <div className="mt-2 text-sm text-red-700">
                      <p>{error}</p>
                    </div>
                  </div>
                </div>
              </div>
            )}

            {isValid && (
              <div className="bg-green-50 border border-green-200 rounded-md p-4">
                <div className="flex">
                  <div className="flex-shrink-0">
                    <svg className="h-5 w-5 text-green-400" viewBox="0 0 20 20" fill="currentColor">
                      <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
                    </svg>
                  </div>
                  <div className="ml-3">
                    <h3 className="text-sm font-medium text-green-800">Configuration Valid</h3>
                    <div className="mt-2 text-sm text-green-700">
                      <p>The configuration is valid and ready to use.</p>
                    </div>
                  </div>
                </div>
              </div>
            )}

            <div className="relative">
              <textarea
                value={editorContent}
                onChange={(e) => handleEditorChange(e.target.value)}
                className="w-full h-96 p-4 font-mono text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                placeholder={`Enter your ${mode.toUpperCase()} configuration here...`}
                spellCheck={false}
              />
            </div>

            <div className="flex justify-between items-center">
              <div className="text-sm text-gray-500">
                Format: {mode.toUpperCase()}
              </div>
              <div className="flex space-x-2">
                <button
                  onClick={() => {
                    navigator.clipboard.writeText(editorContent);
                    alert('Configuration copied to clipboard!');
                  }}
                  className="px-4 py-2 bg-blue-500 text-white rounded-md hover:bg-blue-600 focus:outline-none focus:ring-2 focus:ring-blue-500"
                >
                  Copy
                </button>
                <button
                  onClick={() => {
                    const blob = new Blob([editorContent], { type: 'text/plain' });
                    const url = URL.createObjectURL(blob);
                    const a = document.createElement('a');
                    a.href = url;
                    a.download = `config.${mode}`;
                    document.body.appendChild(a);
                    a.click();
                    document.body.removeChild(a);
                    URL.revokeObjectURL(url);
                  }}
                  className="px-4 py-2 bg-green-500 text-white rounded-md hover:bg-green-600 focus:outline-none focus:ring-2 focus:ring-green-500"
                >
                  Download
                </button>
              </div>
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
              Configuration Editor
            </h3>
            <div className="mt-2 text-sm text-blue-700">
              <p>
                Edit your migration configuration in YAML or JSON format. The configuration will be validated in real-time.
                You can switch between formats using the tabs above.
              </p>
            </div>
          </div>
        </div>
      </div>

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

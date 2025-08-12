import React, { useState, useEffect } from 'react';
import { Card, CardHeader, CardTitle, CardContent } from '../ui/Card';
import { MigrationConfig } from '../../types/configure';

interface ConfigEditorStepProps {
  config: MigrationConfig | null;
  onConfigChange: (config: MigrationConfig | null) => void;
  mode: 'yaml' | 'json';
  onModeChange: (mode: 'yaml' | 'json') => void;
}

export const ConfigEditorStep: React.FC<ConfigEditorStepProps> = ({
  config,
  onConfigChange,
  mode,
  onModeChange
}) => {
  const [editorContent, setEditorContent] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [isValid, setIsValid] = useState(false);

  // Convert config to YAML or JSON string
  const configToString = (config: MigrationConfig, format: 'yaml' | 'json'): string => {
    try {
      if (format === 'yaml') {
        // Simple YAML conversion (you might want to use a proper YAML library)
        const yaml = `name: ${config.name}
description: ${config.description}
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
destination_provider:
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
column_map:
${config.column_map.map(col => `  - name: ${col.name}
    src: ${col.src || 'null'}
    dest: ${col.dest || 'null'}
    dtype: ${col.dtype || 'null'}
    unique_key: ${col.unique_key}
    order_key: ${col.order_key}
    hash_key: ${col.hash_key}
    insert: ${col.insert}
    direction: ${col.direction || 'null'}`).join('\n')}
partition_key: ${config.partition_key || 'null'}
partition_step: ${config.partition_step || 'null'}`;
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
            } else if (key === 'partition_key' || key === 'partition_step') {
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
      const content = configToString(config, mode);
      setEditorContent(content);
      setError(null);
      setIsValid(true);
      // Initialize editedConfig with the current config
      onConfigChange(config);
    }
  }, [config, mode]);

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
      onModeChange(newMode);
      setError(null);
      setIsValid(true);
    }
  };

  if (!config) {
    return (
      <div className="text-center py-8">
        <div className="text-gray-500">No configuration available</div>
      </div>
    );
  }

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
    </div>
  );
};

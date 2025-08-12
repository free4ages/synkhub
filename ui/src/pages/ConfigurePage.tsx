import React, { useState } from 'react';
import { StepperForm, StepperStep } from '../components/ui/Stepper';
import { Card, CardHeader, CardTitle, CardContent } from '../components/ui/Card';
import { LoadingSpinner } from '../components/ui/LoadingSpinner';
import { configureApi } from '../services/configureApi';
import {
  DatabaseConnection,
  ExtractedSchema,
  MigrationConfig,
  ColumnMapping,
  TableInfo,
  EnrichmentTransformation
} from '../types/configure';

// Step Components
import { ConnectionStep } from '../components/configure/ConnectionStep';
import { SchemaExtractionStep } from '../components/configure/SchemaExtractionStep';
import { ConfigurationStep } from '../components/configure/ConfigurationStep';
import { EnrichmentStep } from '../components/configure/EnrichmentStep';
import { ConfigEditorStep } from '../components/configure/ConfigEditorStep';
import { DDLGenerationStep } from '../components/configure/DDLGenerationStep';

const steps: StepperStep[] = [
  {
    id: 'connection',
    title: 'Database Connection',
    description: 'Connect to source database'
  },
  {
    id: 'extraction',
    title: 'Schema Extraction',
    description: 'Extract table schema'
  },
  {
    id: 'configuration',
    title: 'Migration Config',
    description: 'Configure column mapping'
  },
  {
    id: 'enrichment',
    title: 'Enrichment',
    description: 'Define transformations'
  },
  {
    id: 'config-editor',
    title: 'Edit Config',
    description: 'Edit YAML/JSON config'
  },
  {
    id: 'ddl',
    title: 'Generate DDL',
    description: 'Create destination table'
  }
];

export const ConfigurePage: React.FC = () => {
  const [currentStep, setCurrentStep] = useState(0);
  const [isLoading, setIsLoading] = useState(false);
  
  // Step 1: Connection
  const [sourceConnection, setSourceConnection] = useState<DatabaseConnection | null>(null);
  const [tableName, setTableName] = useState('');
  const [tables, setTables] = useState<TableInfo[]>([]);
  
  // Step 2: Schema
  const [extractedSchema, setExtractedSchema] = useState<ExtractedSchema | null>(null);
  const [suggestedConfig, setSuggestedConfig] = useState<MigrationConfig | null>(null);
  
  // Step 3: Configuration
  const [migrationConfig, setMigrationConfig] = useState<MigrationConfig | null>(null);
  
  // Step 4: Enrichment
  const [enrichmentTransformations, setEnrichmentTransformations] = useState<EnrichmentTransformation[]>([]);
  
  // Step 5: Config Editor
  const [editedConfig, setEditedConfig] = useState<MigrationConfig | null>(null);
  const [configEditorMode, setConfigEditorMode] = useState<'yaml' | 'json'>('yaml');
  
  // Step 6: DDL
  const [destinationConnection, setDestinationConnection] = useState<DatabaseConnection | null>(null);
  const [generatedDDL, setGeneratedDDL] = useState('');
  const [configYaml, setConfigYaml] = useState('');
  const [configJson, setConfigJson] = useState('');

  const handleStepChange = (step: number) => {
    setCurrentStep(step);
  };

  const handleNext = async () => {
    if (currentStep === 0) {
      // Validate connection step
      if (!sourceConnection || !tableName) {
        alert('Please fill in all connection details and table name');
        return;
      }
      
      // Extract schema
      setIsLoading(true);
      try {
        const response = await configureApi.extractSchema(sourceConnection, tableName, tables);
        setExtractedSchema(response.schema);
        // Remove suggested_config since it's no longer returned by extract-schema
        setSuggestedConfig(null);
        setMigrationConfig(null);
        setCurrentStep(1);
      } catch (error) {
        console.error('Schema extraction failed:', error);
        alert('Failed to extract schema. Please check your connection details.');
      } finally {
        setIsLoading(false);
      }
    } else if (currentStep === 1) {
      // Validate schema step
      if (!extractedSchema) {
        alert('Schema extraction failed');
        return;
      }
      setCurrentStep(2);
    } else if (currentStep === 2) {
      // Validate configuration step
      if (!migrationConfig) {
        alert('Please configure the migration settings');
        return;
      }
      setCurrentStep(3);
    } else if (currentStep === 3) {
      // Enrichment step - optional, can proceed without transformations
      setCurrentStep(4);
    } else if (currentStep === 4) {
      // Validate config editor step
      if (!editedConfig) {
        alert('Please edit the configuration');
        return;
      }
      
      // Validate config
      setIsLoading(true);
      try {
        const validation = await configureApi.validateConfig(editedConfig);
        if (!validation.valid) {
          alert(`Configuration validation failed:\n${validation.errors.join('\n')}`);
          return;
        }
        setCurrentStep(5);
      } catch (error) {
        console.error('Config validation failed:', error);
        alert('Failed to validate configuration');
      } finally {
        setIsLoading(false);
      }
    }
  };

  const handlePrevious = () => {
    setCurrentStep(Math.max(0, currentStep - 1));
  };

  const handleComplete = async () => {
    if (!editedConfig || !destinationConnection) {
      alert('Please configure destination connection');
      return;
    }

    setIsLoading(true);
    try {
      const response = await configureApi.generateDDL(editedConfig, destinationConnection);
      setGeneratedDDL(response.ddl);
      setConfigYaml(response.config_yaml);
      setConfigJson(response.config_json);
      
      // Show success message
      alert('DDL generated successfully! Check the results below.');
    } catch (error) {
      console.error('DDL generation failed:', error);
      alert('Failed to generate DDL. Please check your configuration.');
    } finally {
      setIsLoading(false);
    }
  };

  const handleEnrichmentColumnMappingsUpdate = (mappings: ColumnMapping[]) => {
    if (migrationConfig) {
      // Add enrichment columns to the existing column mappings
      const updatedColumnMap = [...migrationConfig.column_map, ...mappings];
      setMigrationConfig({
        ...migrationConfig,
        column_map: updatedColumnMap,
        enrichment: {
          enabled: enrichmentTransformations.length > 0,
          transformations: enrichmentTransformations
        }
      });
    }
  };

  const canProceed = () => {
    switch (currentStep) {
      case 0:
        return !!(sourceConnection && tableName);
      case 1:
        return !!extractedSchema;
      case 2:
        return !!migrationConfig;
      case 3:
        return true; // Enrichment is optional
      case 4:
        return !!editedConfig;
      case 5:
        return !!(editedConfig && destinationConnection);
      default:
        return false;
    }
  };

  const renderStepContent = () => {
    switch (currentStep) {
      case 0:
        return (
          <ConnectionStep
            sourceConnection={sourceConnection}
            onSourceConnectionChange={setSourceConnection}
            tableName={tableName}
            onTableNameChange={setTableName}
            tables={tables}
            onTablesChange={setTables}
          />
        );
      case 1:
        return (
          <SchemaExtractionStep
            schema={extractedSchema}
            suggestedConfig={suggestedConfig}
            onSuggestedConfigChange={(config) => {
              setSuggestedConfig(config);
              setMigrationConfig(config);
            }}
          />
        );
      case 2:
        return (
          <ConfigurationStep
            config={migrationConfig}
            onConfigChange={setMigrationConfig}
            schema={extractedSchema}
            availableColumns={extractedSchema?.columns || []}
          />
        );
      case 3:
        return (
          <EnrichmentStep
            transformations={enrichmentTransformations}
            onTransformationsChange={setEnrichmentTransformations}
            availableColumns={extractedSchema?.columns.map(col => col.name) || []}
            onColumnMappingsUpdate={handleEnrichmentColumnMappingsUpdate}
          />
        );
      case 4:
        return (
          <ConfigEditorStep
            config={migrationConfig}
            onConfigChange={setEditedConfig}
            mode={configEditorMode}
            onModeChange={setConfigEditorMode}
          />
        );
      case 5:
        return (
          <DDLGenerationStep
            destinationConnection={destinationConnection}
            onDestinationConnectionChange={setDestinationConnection}
            generatedDDL={generatedDDL}
            configYaml={configYaml}
            configJson={configJson}
          />
        );
      default:
        return <div>Unknown step</div>;
    }
  };

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-gray-900 mb-2">Schema Migration Tool</h1>
        <p className="text-gray-600">
          Extract schema from source database and generate migration configuration with DDL
        </p>
      </div>

      <StepperForm
        steps={steps}
        currentStep={currentStep}
        onStepChange={handleStepChange}
        onNext={handleNext}
        onPrevious={handlePrevious}
        onComplete={handleComplete}
        canProceed={canProceed()}
        isLoading={isLoading}
      >
        {renderStepContent()}
      </StepperForm>
    </div>
  );
};

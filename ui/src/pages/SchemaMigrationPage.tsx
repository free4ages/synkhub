import React, { useState } from 'react';
import { StepperForm, StepperStep } from '../components/ui/Stepper';
import { Card, CardHeader, CardTitle, CardContent } from '../components/ui/Card';
import { LoadingSpinner } from '../components/ui/LoadingSpinner';
import { schemaMigrationApi } from '../services/schemaMigrationApi';
import {
  DatabaseConnection,
  ExtractedSchema,
  MigrationConfig,
  ColumnMapping
} from '../types/schema-migration';

// Step Components
import { ConnectionStep } from '../components/configure/ConnectionStep';
import { SchemaExtractionStep } from '../components/configure/SchemaExtractionStep';
import { ConfigurationStep } from '../components/configure/ConfigurationStep';
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

export const SchemaMigrationPage: React.FC = () => {
  const [currentStep, setCurrentStep] = useState(0);
  const [isLoading, setIsLoading] = useState(false);
  
  // Step 1: Connection
  const [sourceConnection, setSourceConnection] = useState<DatabaseConnection | null>(null);
  const [tableName, setTableName] = useState('');
  
  // Step 2: Schema
  const [extractedSchema, setExtractedSchema] = useState<ExtractedSchema | null>(null);
  const [suggestedConfig, setSuggestedConfig] = useState<MigrationConfig | null>(null);
  
  // Step 3: Configuration
  const [migrationConfig, setMigrationConfig] = useState<MigrationConfig | null>(null);
  
  // Step 4: Config Editor
  const [editedConfig, setEditedConfig] = useState<MigrationConfig | null>(null);
  const [configEditorMode, setConfigEditorMode] = useState<'yaml' | 'json'>('yaml');
  
  // Step 5: DDL
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
        const response = await schemaMigrationApi.extractSchema(sourceConnection, tableName);
        setExtractedSchema(response.schema);
        setSuggestedConfig(response.suggested_config);
        setMigrationConfig(response.suggested_config);
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
      
      // Validate config
      setIsLoading(true);
      try {
        const validation = await schemaMigrationApi.validateConfig(migrationConfig);
        if (!validation.valid) {
          alert(`Configuration validation failed:\n${validation.errors.join('\n')}`);
          return;
        }
        setCurrentStep(3);
      } catch (error) {
        console.error('Config validation failed:', error);
        alert('Failed to validate configuration');
      } finally {
        setIsLoading(false);
      }
    } else if (currentStep === 3) {
      // Validate config editor step
      if (!editedConfig) {
        alert('Please edit the configuration');
        return;
      }
      setCurrentStep(4);
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
      const response = await schemaMigrationApi.generateDDL(editedConfig, destinationConnection);
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

  const canProceed = () => {
    switch (currentStep) {
      case 0:
        return !!(sourceConnection && tableName);
      case 1:
        return !!extractedSchema;
      case 2:
        return !!migrationConfig;
      case 3:
        return !!editedConfig;
      case 4:
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
          />
        );
      case 1:
        return (
          <SchemaExtractionStep
            schema={extractedSchema}
            suggestedConfig={suggestedConfig}
          />
        );
      case 2:
        return (
          <ConfigurationStep
            config={migrationConfig}
            onConfigChange={setMigrationConfig}
            schema={extractedSchema}
          />
        );
      case 3:
        return (
          <ConfigEditorStep
            config={migrationConfig}
            onConfigChange={setEditedConfig}
            mode={configEditorMode}
            onModeChange={setConfigEditorMode}
          />
        );
      case 4:
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

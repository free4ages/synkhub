import axios from 'axios';
import {
  DatabaseConnection,
  ExtractedSchema,
  MigrationConfig,
  SchemaExtractionResponse,
  DDLGenerationResponse,
  TableInfo
} from '../types/configure';

// Create axios instance for schema migration API
const apiClient = axios.create({
  baseURL: '/api',
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
  },
});

export const configureApi = {
  async extractSchema(
    sourceConnection: DatabaseConnection, 
    tableName: string, 
    additionalTables: TableInfo[] = []
  ): Promise<SchemaExtractionResponse> {
    const response = await apiClient.post('/configure/extract-schema', {
      source_connection: sourceConnection,
      table_name: tableName,
      additional_tables: additionalTables
    });
    return response.data;
  },

  async generateDDL(config: MigrationConfig, destinationConnection: DatabaseConnection): Promise<DDLGenerationResponse> {
    const response = await apiClient.post('/configure/generate-ddl', {
      config,
      destination_connection: destinationConnection
    });
    return response.data;
  },

  async validateConfig(config: MigrationConfig): Promise<{ valid: boolean; errors: string[] }> {
    const response = await apiClient.post('/configure/validate-config', config);
    return response.data;
  },

  async generateSuggestedConfig(
    schema: ExtractedSchema, 
    selectedColumns: string[]
  ): Promise<MigrationConfig> {
    const response = await apiClient.post('/configure/generate-suggested-config', {
      schema_data: schema,
      selected_columns: selectedColumns
    });
    return response.data;
  }
};

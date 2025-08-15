import { 
  DataStoreResponse, 
  DataStorageResponse, 
  DataStoreTypesSummary, 
  DataStoreTagsSummary,
  ConnectionTestResponse 
} from '../types/api';

const API_BASE = '/api';

class DataStoresApi {
  /**
   * Get all available datastores
   */
  async getDataStores(): Promise<DataStorageResponse> {
    const response = await fetch(`${API_BASE}/datastores/`);
    if (!response.ok) {
      throw new Error(`Failed to fetch datastores: ${response.statusText}`);
    }
    return response.json();
  }

  /**
   * Get details of a specific datastore
   */
  async getDataStore(datastoreName: string): Promise<DataStoreResponse> {
    const response = await fetch(`${API_BASE}/datastores/${encodeURIComponent(datastoreName)}`);
    if (!response.ok) {
      if (response.status === 404) {
        throw new Error(`Datastore '${datastoreName}' not found`);
      }
      throw new Error(`Failed to fetch datastore: ${response.statusText}`);
    }
    return response.json();
  }

  /**
   * Get summary of datastores grouped by type
   */
  async getDataStoreTypesSummary(): Promise<DataStoreTypesSummary> {
    const response = await fetch(`${API_BASE}/datastores/types/summary`);
    if (!response.ok) {
      throw new Error(`Failed to fetch datastore types summary: ${response.statusText}`);
    }
    return response.json();
  }

  /**
   * Get summary of datastores grouped by tags
   */
  async getDataStoreTagsSummary(): Promise<DataStoreTagsSummary> {
    const response = await fetch(`${API_BASE}/datastores/tags/summary`);
    if (!response.ok) {
      throw new Error(`Failed to fetch datastore tags summary: ${response.statusText}`);
    }
    return response.json();
  }

  /**
   * Test connection to a specific datastore
   */
  async testDataStoreConnection(datastoreName: string): Promise<ConnectionTestResponse> {
    const response = await fetch(
      `${API_BASE}/datastores/test-connection/${encodeURIComponent(datastoreName)}`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
      }
    );
    if (!response.ok) {
      if (response.status === 404) {
        throw new Error(`Datastore '${datastoreName}' not found`);
      }
      throw new Error(`Failed to test connection: ${response.statusText}`);
    }
    return response.json();
  }

  /**
   * Get datastores filtered by type
   */
  async getDataStoresByType(type: string): Promise<DataStoreResponse[]> {
    const allDataStores = await this.getDataStores();
    return allDataStores.datastores.filter(ds => ds.type === type);
  }

  /**
   * Get datastores filtered by tag
   */
  async getDataStoresByTag(tag: string): Promise<DataStoreResponse[]> {
    const allDataStores = await this.getDataStores();
    return allDataStores.datastores.filter(ds => ds.tags.includes(tag));
  }

  /**
   * Search datastores by name or description
   */
  async searchDataStores(query: string): Promise<DataStoreResponse[]> {
    const allDataStores = await this.getDataStores();
    const lowerQuery = query.toLowerCase();
    return allDataStores.datastores.filter(ds => 
      ds.name.toLowerCase().includes(lowerQuery) ||
      (ds.description && ds.description.toLowerCase().includes(lowerQuery))
    );
  }
}

export const dataStoresApi = new DataStoresApi();
export default dataStoresApi;

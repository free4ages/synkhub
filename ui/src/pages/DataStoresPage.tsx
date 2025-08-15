import React, { useState, useEffect } from 'react';
import { 
  Server, 
  Database, 
  Search, 
  Filter, 
  CheckCircle, 
  XCircle, 
  Loader2, 
  Tag,
  Eye,
  TestTube,
  AlertCircle,
  Info
} from 'lucide-react';
import { Card } from '../components/ui/Card';
import { StatusBadge } from '../components/ui/StatusBadge';
import { LoadingSpinner } from '../components/ui/LoadingSpinner';
import { dataStoresApi } from '../services/datastoresApi';
import { 
  DataStoreResponse, 
  DataStorageResponse, 
  DataStoreTypesSummary,
  ConnectionTestResponse,
  DATASTORE_TYPES 
} from '../types/api';

export const DataStoresPage: React.FC = () => {
  const [datastores, setDatastores] = useState<DataStoreResponse[]>([]);
  const [filteredDatastores, setFilteredDatastores] = useState<DataStoreResponse[]>([]);
  const [typesSummary, setTypesSummary] = useState<DataStoreTypesSummary>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedType, setSelectedType] = useState<string>('all');
  const [selectedDatastore, setSelectedDatastore] = useState<DataStoreResponse | null>(null);
  const [testingConnections, setTestingConnections] = useState<Set<string>>(new Set());
  const [connectionResults, setConnectionResults] = useState<Map<string, ConnectionTestResponse>>(new Map());

  useEffect(() => {
    loadDataStores();
  }, []);

  useEffect(() => {
    filterDatastores();
  }, [datastores, searchQuery, selectedType]);

  const loadDataStores = async () => {
    try {
      setLoading(true);
      setError(null);
      
      const [datastoresResponse, typesSummaryResponse] = await Promise.all([
        dataStoresApi.getDataStores(),
        dataStoresApi.getDataStoreTypesSummary()
      ]);
      
      setDatastores(datastoresResponse.datastores);
      setTypesSummary(typesSummaryResponse);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load datastores');
    } finally {
      setLoading(false);
    }
  };

  const filterDatastores = () => {
    let filtered = datastores;

    // Filter by search query
    if (searchQuery) {
      const query = searchQuery.toLowerCase();
      filtered = filtered.filter(ds => 
        ds.name.toLowerCase().includes(query) ||
        ds.type.toLowerCase().includes(query) ||
        (ds.description && ds.description.toLowerCase().includes(query)) ||
        ds.tags.some(tag => tag.toLowerCase().includes(query))
      );
    }

    // Filter by type
    if (selectedType !== 'all') {
      filtered = filtered.filter(ds => ds.type === selectedType);
    }

    setFilteredDatastores(filtered);
  };

  const testConnection = async (datastoreName: string) => {
    setTestingConnections(prev => new Set(prev).add(datastoreName));
    
    try {
      const result = await dataStoresApi.testDataStoreConnection(datastoreName);
      setConnectionResults(prev => new Map(prev).set(datastoreName, result));
    } catch (err) {
      const errorResult: ConnectionTestResponse = {
        status: 'error',
        message: err instanceof Error ? err.message : 'Connection test failed'
      };
      setConnectionResults(prev => new Map(prev).set(datastoreName, errorResult));
    } finally {
      setTestingConnections(prev => {
        const newSet = new Set(prev);
        newSet.delete(datastoreName);
        return newSet;
      });
    }
  };

  const getTypeIcon = (type: string) => {
    switch (type) {
      case DATASTORE_TYPES.POSTGRES:
        return <Database className="h-4 w-4 text-blue-500" />;
      case DATASTORE_TYPES.MYSQL:
        return <Database className="h-4 w-4 text-orange-500" />;
      case DATASTORE_TYPES.CLICKHOUSE:
        return <Database className="h-4 w-4 text-yellow-500" />;
      case DATASTORE_TYPES.DUCKDB:
        return <Database className="h-4 w-4 text-green-500" />;
      case DATASTORE_TYPES.OBJECT_STORAGE:
        return <Server className="h-4 w-4 text-purple-500" />;
      default:
        return <Server className="h-4 w-4 text-gray-500" />;
    }
  };

  const getTypeColor = (type: string) => {
    switch (type) {
      case DATASTORE_TYPES.POSTGRES:
        return 'bg-blue-100 text-blue-800';
      case DATASTORE_TYPES.MYSQL:
        return 'bg-orange-100 text-orange-800';
      case DATASTORE_TYPES.CLICKHOUSE:
        return 'bg-yellow-100 text-yellow-800';
      case DATASTORE_TYPES.DUCKDB:
        return 'bg-green-100 text-green-800';
      case DATASTORE_TYPES.OBJECT_STORAGE:
        return 'bg-purple-100 text-purple-800';
      default:
        return 'bg-gray-100 text-gray-800';
    }
  };

  const renderConnectionSummary = (connection: Record<string, any>) => {
    const relevantFields = ['host', 'port', 'database', 'dbname', 's3_bucket', 'region'];
    const displayFields = Object.entries(connection)
      .filter(([key, value]) => relevantFields.includes(key) && value !== null && value !== undefined)
      .slice(0, 3); // Show only first 3 relevant fields

    return (
      <div className="text-xs text-gray-500 space-y-1">
        {displayFields.map(([key, value]) => (
          <div key={key} className="flex justify-between">
            <span className="font-medium">{key}:</span>
            <span>{String(value)}</span>
          </div>
        ))}
      </div>
    );
  };

  if (loading) {
    return (
      <div className="p-6">
        <LoadingSpinner />
      </div>
    );
  }

  if (error) {
    return (
      <div className="p-6">
        <Card className="p-6">
          <div className="flex items-center space-x-2 text-red-600">
            <AlertCircle className="h-5 w-5" />
            <span className="font-medium">Error loading datastores</span>
          </div>
          <p className="mt-2 text-sm text-gray-600">{error}</p>
          <button
            onClick={loadDataStores}
            className="mt-4 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
          >
            Retry
          </button>
        </Card>
      </div>
    );
  }

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">DataStores</h1>
          <p className="text-gray-600">Manage centralized database connections</p>
        </div>
        <div className="flex items-center space-x-2">
          <span className="text-sm text-gray-500">
            {filteredDatastores.length} of {datastores.length} datastores
          </span>
        </div>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-5 gap-4">
        <Card className="p-4">
          <div className="flex items-center space-x-2">
            <Server className="h-5 w-5 text-blue-500" />
            <div>
              <p className="text-sm font-medium text-gray-900">Total</p>
              <p className="text-2xl font-bold text-blue-600">{datastores.length}</p>
            </div>
          </div>
        </Card>
        
        {Object.entries(typesSummary).map(([type, summary]) => (
          <Card key={type} className="p-4">
            <div className="flex items-center space-x-2">
              {getTypeIcon(type)}
              <div>
                <p className="text-sm font-medium text-gray-900 capitalize">{type}</p>
                <p className="text-2xl font-bold text-gray-900">{summary.count}</p>
              </div>
            </div>
          </Card>
        ))}
      </div>

      {/* Filters */}
      <div className="flex flex-col sm:flex-row gap-4">
        <div className="flex-1 relative">
          <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400" />
          <input
            type="text"
            placeholder="Search datastores..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          />
        </div>
        
        <div className="relative">
          <Filter className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400" />
          <select
            value={selectedType}
            onChange={(e) => setSelectedType(e.target.value)}
            className="pl-10 pr-8 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent appearance-none bg-white"
          >
            <option value="all">All Types</option>
            {Object.entries(typesSummary).map(([type, summary]) => (
              <option key={type} value={type}>
                {type} ({summary.count})
              </option>
            ))}
          </select>
        </div>
      </div>

      {/* DataStores Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {filteredDatastores.map((datastore) => {
          const connectionResult = connectionResults.get(datastore.name);
          const isTesting = testingConnections.has(datastore.name);
          
          return (
            <Card key={datastore.name} className="p-6 hover:shadow-lg transition-shadow">
              <div className="flex items-start justify-between mb-4">
                <div className="flex items-center space-x-3">
                  {getTypeIcon(datastore.type)}
                  <div>
                    <h3 className="font-semibold text-gray-900">{datastore.name}</h3>
                    <span className={`inline-block px-2 py-1 rounded-full text-xs font-medium ${getTypeColor(datastore.type)}`}>
                      {datastore.type}
                    </span>
                  </div>
                </div>
                
                <div className="flex space-x-2">
                  <button
                    onClick={() => setSelectedDatastore(datastore)}
                    className="p-1 text-gray-400 hover:text-blue-600 transition-colors"
                    title="View details"
                  >
                    <Eye className="h-4 w-4" />
                  </button>
                  <button
                    onClick={() => testConnection(datastore.name)}
                    disabled={isTesting}
                    className="p-1 text-gray-400 hover:text-green-600 transition-colors disabled:opacity-50"
                    title="Test connection"
                  >
                    {isTesting ? (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    ) : (
                      <TestTube className="h-4 w-4" />
                    )}
                  </button>
                </div>
              </div>

              {datastore.description && (
                <p className="text-sm text-gray-600 mb-4">{datastore.description}</p>
              )}

              {/* Connection Summary */}
              <div className="mb-4">
                <h4 className="text-xs font-medium text-gray-700 mb-2">Connection</h4>
                {renderConnectionSummary(datastore.connection_summary)}
              </div>

              {/* Tags */}
              {datastore.tags.length > 0 && (
                <div className="mb-4">
                  <div className="flex flex-wrap gap-1">
                    {datastore.tags.map((tag) => (
                      <span
                        key={tag}
                        className="inline-flex items-center px-2 py-1 rounded-full text-xs bg-gray-100 text-gray-700"
                      >
                        <Tag className="h-3 w-3 mr-1" />
                        {tag}
                      </span>
                    ))}
                  </div>
                </div>
              )}

              {/* Connection Test Result */}
              {connectionResult && (
                <div className={`flex items-center space-x-2 p-2 rounded-lg text-sm ${
                  connectionResult.status === 'success' 
                    ? 'bg-green-50 text-green-800' 
                    : 'bg-red-50 text-red-800'
                }`}>
                  {connectionResult.status === 'success' ? (
                    <CheckCircle className="h-4 w-4" />
                  ) : (
                    <XCircle className="h-4 w-4" />
                  )}
                  <span className="text-xs">{connectionResult.message}</span>
                </div>
              )}
            </Card>
          );
        })}
      </div>

      {filteredDatastores.length === 0 && (
        <Card className="p-8 text-center">
          <Server className="h-12 w-12 text-gray-400 mx-auto mb-4" />
          <h3 className="text-lg font-medium text-gray-900 mb-2">No datastores found</h3>
          <p className="text-gray-600">
            {searchQuery || selectedType !== 'all' 
              ? 'Try adjusting your search or filter criteria.'
              : 'No datastores are configured yet.'}
          </p>
        </Card>
      )}

      {/* DataStore Detail Modal */}
      {selectedDatastore && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50">
          <div className="bg-white rounded-lg max-w-2xl w-full max-h-[80vh] overflow-y-auto">
            <div className="p-6">
              <div className="flex items-center justify-between mb-6">
                <div className="flex items-center space-x-3">
                  {getTypeIcon(selectedDatastore.type)}
                  <div>
                    <h2 className="text-xl font-bold text-gray-900">{selectedDatastore.name}</h2>
                    <span className={`inline-block px-2 py-1 rounded-full text-sm font-medium ${getTypeColor(selectedDatastore.type)}`}>
                      {selectedDatastore.type}
                    </span>
                  </div>
                </div>
                <button
                  onClick={() => setSelectedDatastore(null)}
                  className="text-gray-400 hover:text-gray-600"
                >
                  <XCircle className="h-6 w-6" />
                </button>
              </div>

              {selectedDatastore.description && (
                <div className="mb-6">
                  <h3 className="text-sm font-medium text-gray-700 mb-2">Description</h3>
                  <p className="text-gray-600">{selectedDatastore.description}</p>
                </div>
              )}

              <div className="mb-6">
                <h3 className="text-sm font-medium text-gray-700 mb-2">Connection Details</h3>
                <div className="bg-gray-50 rounded-lg p-4">
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    {Object.entries(selectedDatastore.connection_summary).map(([key, value]) => (
                      <div key={key}>
                        <span className="text-sm font-medium text-gray-700">{key}:</span>
                        <span className="ml-2 text-sm text-gray-600">{String(value)}</span>
                      </div>
                    ))}
                  </div>
                </div>
              </div>

              {selectedDatastore.tags.length > 0 && (
                <div className="mb-6">
                  <h3 className="text-sm font-medium text-gray-700 mb-2">Tags</h3>
                  <div className="flex flex-wrap gap-2">
                    {selectedDatastore.tags.map((tag) => (
                      <span
                        key={tag}
                        className="inline-flex items-center px-3 py-1 rounded-full text-sm bg-blue-100 text-blue-800"
                      >
                        <Tag className="h-3 w-3 mr-1" />
                        {tag}
                      </span>
                    ))}
                  </div>
                </div>
              )}

              <div className="flex justify-end space-x-3">
                <button
                  onClick={() => testConnection(selectedDatastore.name)}
                  disabled={testingConnections.has(selectedDatastore.name)}
                  className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50 flex items-center space-x-2"
                >
                  {testingConnections.has(selectedDatastore.name) ? (
                    <>
                      <Loader2 className="h-4 w-4 animate-spin" />
                      <span>Testing...</span>
                    </>
                  ) : (
                    <>
                      <TestTube className="h-4 w-4" />
                      <span>Test Connection</span>
                    </>
                  )}
                </button>
                <button
                  onClick={() => setSelectedDatastore(null)}
                  className="px-4 py-2 bg-gray-600 text-white rounded-lg hover:bg-gray-700"
                >
                  Close
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

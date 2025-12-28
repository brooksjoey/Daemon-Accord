import { getConnectors } from '@/lib/api/client';
import { Card } from '@/components/ui/Card';
import { Button } from '@/components/ui/Button';
import { Badge } from '@/components/ui/Badge';
import { Plus } from 'lucide-react';

export const dynamic = 'force-dynamic';

export default async function ConnectorsPage() {
  const connectors = await getConnectors();

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Connectors</h1>
          <p className="mt-1 text-sm text-gray-500">
            Manage integrations and data connectors
          </p>
        </div>
        <Button>
          <Plus className="mr-2 h-4 w-4" />
          Add Connector
        </Button>
      </div>

      {connectors.length === 0 ? (
        <Card>
          <div className="text-center py-12">
            <p className="text-gray-500 mb-4">No connectors configured</p>
            <Button>Add Your First Connector</Button>
          </div>
        </Card>
      ) : (
        <div className="grid grid-cols-1 gap-4 md:grid-cols-2 lg:grid-cols-3">
          {connectors.map((connector) => (
            <Card key={connector.id}>
              <div className="flex items-start justify-between mb-2">
                <h3 className="text-lg font-semibold text-gray-900">{connector.name}</h3>
                <Badge
                  variant={
                    connector.status === 'connected' ? 'success' :
                    connector.status === 'error' ? 'error' : 'warning'
                  }
                >
                  {connector.status}
                </Badge>
              </div>
              <p className="text-sm text-gray-600 mb-4">Type: {connector.type}</p>
              <div className="flex items-center justify-between text-xs text-gray-500">
                <span className="font-mono">{connector.id}</span>
                {connector.lastSync && (
                  <span>Last sync: {new Date(connector.lastSync).toLocaleDateString()}</span>
                )}
              </div>
            </Card>
          ))}
        </div>
      )}
    </div>
  );
}

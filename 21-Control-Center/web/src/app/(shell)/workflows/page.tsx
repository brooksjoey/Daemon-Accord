import { getWorkflows } from '@/lib/api/client';
import { Card } from '@/components/ui/Card';
import { Button } from '@/components/ui/Button';
import { Badge } from '@/components/ui/Badge';
import { Plus } from 'lucide-react';

export const dynamic = 'force-dynamic';

export default async function WorkflowsPage() {
  const workflows = await getWorkflows();

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Workflows</h1>
          <p className="mt-1 text-sm text-gray-500">
            Manage your automation workflows
          </p>
        </div>
        <Button>
          <Plus className="mr-2 h-4 w-4" />
          Create Workflow
        </Button>
      </div>

      {workflows.length === 0 ? (
        <Card>
          <div className="text-center py-12">
            <p className="text-gray-500 mb-4">No workflows yet</p>
            <Button>Create Your First Workflow</Button>
          </div>
        </Card>
      ) : (
        <div className="grid grid-cols-1 gap-4 md:grid-cols-2 lg:grid-cols-3">
          {workflows.map((workflow) => (
            <Card key={workflow.name}>
              <div className="flex items-start justify-between mb-2">
                <h3 className="text-lg font-semibold text-gray-900">{workflow.name}</h3>
                <Badge variant={workflow.status === 'active' ? 'success' : 'default'}>
                  {workflow.status}
                </Badge>
              </div>
              <p className="text-sm text-gray-600 mb-4">{workflow.description}</p>
              <div className="flex items-center justify-between text-xs text-gray-500">
                <span>v{workflow.version}</span>
                {workflow.lastRun && (
                  <span>Last run: {new Date(workflow.lastRun).toLocaleDateString()}</span>
                )}
              </div>
            </Card>
          ))}
        </div>
      )}
    </div>
  );
}

import { getWorkflows, getRuns, isMockMode } from '@/lib/api/client';
import { Card } from '@/components/ui/Card';
import { Badge } from '@/components/ui/Badge';

export const dynamic = 'force-dynamic';

export default async function DashboardPage() {
  const workflows = await getWorkflows();
  const runs = await getRuns();
  const mockMode = isMockMode();

  const activeWorkflows = workflows.filter(w => w.status === 'active').length;
  const recentRuns = runs.slice(0, 5);
  const completedRuns = runs.filter(r => r.status === 'completed').length;
  const failedRuns = runs.filter(r => r.status === 'failed').length;

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-gray-900">Dashboard</h1>
        <p className="mt-1 text-sm text-gray-500">
          Overview of your Daemon Accord platform
        </p>
      </div>

      {mockMode && (
        <div className="rounded-lg border border-yellow-200 bg-yellow-50 p-4">
          <div className="flex items-center gap-2">
            <Badge variant="warning">Mock Mode Active</Badge>
            <span className="text-sm text-yellow-800">
              Displaying mock data. Connect to backend to see real data.
            </span>
          </div>
        </div>
      )}

      <div className="grid grid-cols-1 gap-6 md:grid-cols-3">
        <Card>
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-gray-600">Active Workflows</p>
              <p className="mt-1 text-3xl font-bold text-gray-900">{activeWorkflows}</p>
            </div>
          </div>
        </Card>

        <Card>
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-gray-600">Completed Runs</p>
              <p className="mt-1 text-3xl font-bold text-green-600">{completedRuns}</p>
            </div>
          </div>
        </Card>

        <Card>
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-gray-600">Failed Runs</p>
              <p className="mt-1 text-3xl font-bold text-red-600">{failedRuns}</p>
            </div>
          </div>
        </Card>
      </div>

      <Card>
        <h2 className="text-lg font-semibold text-gray-900 mb-4">Recent Runs</h2>
        {recentRuns.length === 0 ? (
          <div className="text-center py-8 text-gray-500">
            <p>No runs yet</p>
            <p className="text-sm mt-1">Create a workflow to get started</p>
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Run ID</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Workflow</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Status</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Started</th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {recentRuns.map((run) => (
                  <tr key={run.runId}>
                    <td className="px-4 py-3 text-sm font-mono text-gray-900">{run.runId}</td>
                    <td className="px-4 py-3 text-sm text-gray-600">{run.workflowName}</td>
                    <td className="px-4 py-3 text-sm">
                      <Badge
                        variant={
                          run.status === 'completed' ? 'success' :
                          run.status === 'failed' ? 'error' :
                          run.status === 'running' ? 'default' : 'warning'
                        }
                      >
                        {run.status}
                      </Badge>
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-600">
                      {new Date(run.startedAt).toLocaleString()}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>
    </div>
  );
}

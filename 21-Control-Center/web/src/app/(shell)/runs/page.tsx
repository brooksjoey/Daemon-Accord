import { getRuns } from '@/lib/api/client';
import { Card } from '@/components/ui/Card';
import { Badge } from '@/components/ui/Badge';

export const dynamic = 'force-dynamic';

export default async function RunsPage() {
  const runs = await getRuns();

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-gray-900">Runs</h1>
        <p className="mt-1 text-sm text-gray-500">
          View execution history and job runs
        </p>
      </div>

      {runs.length === 0 ? (
        <Card>
          <div className="text-center py-12">
            <p className="text-gray-500 mb-2">No runs yet</p>
            <p className="text-sm text-gray-400">Workflow executions will appear here</p>
          </div>
        </Card>
      ) : (
        <Card>
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Run ID</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Workflow</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Status</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Started</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Duration</th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {runs.map((run) => (
                  <tr key={run.runId} className="hover:bg-gray-50">
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
                    <td className="px-4 py-3 text-sm text-gray-600">
                      {run.duration ? `${run.duration}s` : '-'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      )}
    </div>
  );
}

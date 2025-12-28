import { isMockMode } from '@/lib/api/client';
import { Card } from '@/components/ui/Card';
import { Badge } from '@/components/ui/Badge';

export const dynamic = 'force-dynamic';

export default function SettingsPage() {
  const mockMode = isMockMode();
  const apiBase = process.env.NEXT_PUBLIC_DAEMON_ACCORD_API_BASE || 'http://localhost:8082';

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-gray-900">Settings</h1>
        <p className="mt-1 text-sm text-gray-500">
          Configure environment and integrations
        </p>
      </div>

      <Card>
        <h2 className="text-lg font-semibold text-gray-900 mb-4">Environment</h2>
        <div className="space-y-4">
          <div className="flex items-center justify-between py-2 border-b">
            <span className="text-sm font-medium text-gray-700">API Mode</span>
            <Badge variant={mockMode ? 'warning' : 'success'}>
              {mockMode ? 'Mock Mode' : 'Live Mode'}
            </Badge>
          </div>
          <div className="flex items-center justify-between py-2 border-b">
            <span className="text-sm font-medium text-gray-700">Backend URL</span>
            <span className="text-sm text-gray-600 font-mono">{apiBase}</span>
          </div>
          <div className="flex items-center justify-between py-2">
            <span className="text-sm font-medium text-gray-700">Node Environment</span>
            <span className="text-sm text-gray-600">{process.env.NODE_ENV || 'development'}</span>
          </div>
        </div>
      </Card>

      <Card>
        <h2 className="text-lg font-semibold text-gray-900 mb-4">Integration Status</h2>
        <div className="space-y-3">
          <div className="flex items-center justify-between py-2">
            <span className="text-sm text-gray-700">Control Plane API</span>
            <Badge variant={mockMode ? 'warning' : 'success'}>
              {mockMode ? 'Not Connected' : 'Connected'}
            </Badge>
          </div>
          <div className="flex items-center justify-between py-2">
            <span className="text-sm text-gray-700">Database</span>
            <Badge variant="default">Not Configured</Badge>
          </div>
          <div className="flex items-center justify-between py-2">
            <span className="text-sm text-gray-700">Authentication</span>
            <Badge variant="default">Not Configured</Badge>
          </div>
        </div>
      </Card>

      <Card>
        <h2 className="text-lg font-semibold text-gray-900 mb-4">Configuration</h2>
        <div className="text-sm text-gray-600 space-y-2">
          <p>To switch to live mode, set environment variable:</p>
          <code className="block bg-gray-100 p-2 rounded font-mono text-xs">
            NEXT_PUBLIC_MOCK_API=false
          </code>
          <p className="mt-2">And configure backend URL:</p>
          <code className="block bg-gray-100 p-2 rounded font-mono text-xs">
            NEXT_PUBLIC_DAEMON_ACCORD_API_BASE=http://localhost:8082
          </code>
        </div>
      </Card>
    </div>
  );
}

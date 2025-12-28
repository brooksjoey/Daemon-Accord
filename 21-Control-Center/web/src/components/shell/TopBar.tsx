import { isMockMode } from '@/lib/api/client';
import { Badge } from '@/components/ui/Badge';

export function TopBar() {
  const mockMode = isMockMode();

  return (
    <div className="flex h-16 items-center justify-between border-b bg-white px-6">
      <div>
        <h2 className="text-xl font-semibold text-gray-900">Control Center</h2>
      </div>
      <div className="flex items-center gap-4">
        {mockMode && (
          <Badge variant="warning">Mock Mode</Badge>
        )}
        <div className="text-sm text-gray-500">
          Environment: {process.env.NODE_ENV || 'development'}
        </div>
      </div>
    </div>
  );
}

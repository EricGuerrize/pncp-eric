import { Info, LoaderCircle, Server, TriangleAlert, WifiOff } from 'lucide-react';
import type { JobProgress } from '../types';

function errorIcon(code?: string) {
  if (code?.startsWith('oracle_')) return <WifiOff className="h-5 w-5 text-rose-600" />;
  return <TriangleAlert className="h-5 w-5 text-rose-600" />;
}

export function ProgressPanel({ progress, error }: { progress: JobProgress; error: string }) {
  if (progress.status === 'idle' || progress.status === 'done') return null;

  const percent = progress.total > 0 ? Math.round((progress.current / progress.total) * 100) : 0;
  const isError = Boolean(error);

  return (
    <div className={`mb-5 rounded-xl border p-4 ${isError ? 'border-rose-200 bg-rose-50' : 'border-blue-200 bg-blue-50'}`} role="status">
      <div className="flex items-start gap-3">
        {isError ? errorIcon(progress.errorCode) : <LoaderCircle className="h-5 w-5 animate-spin text-blue-600" />}
        <div className="min-w-0 flex-1">
          <div className="flex justify-between gap-3 text-sm font-semibold">
            <span>{error || progress.message}</span>
            <span>{progress.total > 0 && !isError ? `${percent}%` : ''}</span>
          </div>

          {!isError && (
            <div className="mt-2 h-2 overflow-hidden rounded-full bg-blue-100">
              <div className="h-full rounded-full bg-blue-600 transition-all" style={{ width: progress.total > 0 ? `${percent}%` : '12%' }} />
            </div>
          )}

          {isError && progress.errorHint && (
            <div className="mt-3 rounded-lg border border-rose-200 bg-white/80 px-3 py-2 text-sm text-rose-800">
              <div className="flex items-start gap-2">
                <Info className="mt-0.5 h-4 w-4 shrink-0" />
                <span>{progress.errorHint}</span>
              </div>
            </div>
          )}
        </div>
        <Server className={`hidden h-5 w-5 sm:block ${isError ? 'text-rose-300' : 'text-blue-400'}`} />
      </div>
    </div>
  );
}

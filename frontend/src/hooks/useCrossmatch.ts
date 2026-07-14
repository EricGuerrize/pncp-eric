import { useCallback, useEffect, useRef, useState } from 'react';
import { runCrossmatch } from '../services/api';
import type { CrossmatchData, JobProgress, RunMeta } from '../types';

const emptyData: CrossmatchData = { ambos: [], apenas_pncp: [], apenas_aplic: [] };
const initialProgress: JobProgress = { status: 'idle', stage: '', message: '', current: 0, total: 0 };

type CrossmatchError = Error & { code?: string; hint?: string };

export function useCrossmatch() {
  const [data, setData] = useState<CrossmatchData>(emptyData);
  const [progress, setProgress] = useState<JobProgress>(initialProgress);
  const [error, setError] = useState('');
  const [lastRun, setLastRun] = useState<RunMeta | null>(null);
  const controller = useRef<AbortController | null>(null);

  const execute = useCallback(async (municipio: string, ano: string, force = false) => {
    controller.current?.abort();
    controller.current = new AbortController();

    const startedAt = new Date().toISOString();
    setError('');
    setLastRun({ municipio, ano, startedAt, status: 'running' });
    setProgress({ status: 'running', stage: 'start', message: 'Iniciando analise...', current: 0, total: 0 });

    try {
      const result = await runCrossmatch(municipio, ano, force, setProgress, controller.current.signal);
      setData(result);
      setLastRun((prev) => prev ? {
        ...prev,
        status: 'done',
        finishedAt: new Date().toISOString(),
        cachedAt: progress.cachedAt,
      } : null);
    } catch (err) {
      if (err instanceof DOMException && err.name === 'AbortError') return;

      const typed = err as CrossmatchError;
      const message = err instanceof Error ? err.message : 'Nao foi possivel executar o cruzamento.';
      setError(message);
      setProgress((prev) => ({
        ...prev,
        status: 'error',
        message,
        errorCode: typed.code,
        errorHint: typed.hint,
      }));
      setLastRun((prev) => prev ? { ...prev, status: 'error', finishedAt: new Date().toISOString() } : null);
    }
  }, [progress.cachedAt]);

  useEffect(() => {
    if (progress.status === 'done') {
      setLastRun((prev) => prev ? {
        ...prev,
        status: 'done',
        finishedAt: new Date().toISOString(),
        cachedAt: progress.cachedAt,
      } : null);
    }
  }, [progress.cachedAt, progress.status]);

  useEffect(() => () => controller.current?.abort(), []);
  return { data, progress, error, lastRun, execute };
}

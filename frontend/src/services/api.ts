import type { CrossmatchData, JobProgress, Licitacao } from '../types';

const API_BASE = import.meta.env.VITE_API_URL || '/api/live-crossmatch';

interface RawRecord extends Record<string, unknown> {
  orgao?: string;
  modalidade?: string;
  numero?: string;
  ano?: string | number;
  objeto?: string;
  valor?: number;
  cnpj?: string;
}

interface JobStatus extends JobProgress {
  data?: { ambos?: RawRecord[]; apenas_pncp?: RawRecord[]; apenas_aplic?: RawRecord[] };
  error?: string;
}

function createJobError(job: JobStatus): Error {
  const error = new Error(job.message || job.error || 'O cruzamento falhou.');
  (error as Error & { code?: string; hint?: string }).code = job.errorCode;
  (error as Error & { code?: string; hint?: string }).hint = job.errorHint;
  return error;
}

const text = (value: unknown) => value == null ? '' : String(value);
const number = (value: unknown) => Number.isFinite(Number(value)) ? Number(value) : 0;

function addBusinessDays(iso: string, days: number): string | undefined {
  const date = new Date(`${iso}T12:00:00`);
  if (Number.isNaN(date.getTime())) return undefined;
  let added = 0;
  while (added < days) {
    date.setDate(date.getDate() + 1);
    if (date.getDay() !== 0 && date.getDay() !== 6) added++;
  }
  return date.toISOString().slice(0, 10);
}

function businessDaysUntil(iso?: string): number | undefined {
  if (!iso) return undefined;
  const target = new Date(`${iso}T12:00:00`);
  const cursor = new Date();
  cursor.setHours(12, 0, 0, 0);
  if (Number.isNaN(target.getTime())) return undefined;
  const direction = target >= cursor ? 1 : -1;
  let count = 0;
  while (cursor.toDateString() !== target.toDateString()) {
    cursor.setDate(cursor.getDate() + direction);
    if (cursor.getDay() !== 0 && cursor.getDay() !== 6) count += direction;
  }
  return count;
}

function normalize(raw: RawRecord, bucket: keyof CrossmatchData, index: number): Licitacao {
  const dataPNCP = text(raw.dataPNCP || raw.dataPublicacaoPncp) || undefined;
  const dataAPLIC = text(raw.dataAPLIC) || undefined;
  const prazoAplic = bucket === 'apenas_pncp' && dataPNCP ? addBusinessDays(dataPNCP, 5) : undefined;
  const diasUteisRestantes = businessDaysUntil(prazoAplic);
  const statusPrazo = bucket !== 'apenas_pncp' ? 'nao_aplicavel'
    : diasUteisRestantes == null ? 'sem_data'
    : diasUteisRestantes < 0 ? 'vencido'
    : diasUteisRestantes === 0 ? 'vence_hoje' : 'aguardando';

  return {
    id: text(raw.id || `${bucket}-${raw.cnpj}-${raw.numero}-${raw.ano}-${index}`),
    orgao: text(raw.orgao), modalidade: text(raw.modalidade), numero: text(raw.numero),
    ano: text(raw.ano), objeto: text(raw.objeto), valor: number(raw.valor), cnpj: text(raw.cnpj),
    numeroControlePNCP: text(raw.numeroControlePNCP) || undefined,
    dataPNCP, dataAPLIC, prazoAplic, diasUteisRestantes, statusPrazo,
    alertaAtivo: statusPrazo === 'vencido',
    orgaoAplic: text(raw.orgao_aplic), numeroAplic: text(raw.numero_aplic),
    objetoAplic: text(raw.objeto_aplic), valorAplic: number(raw.valor_aplic),
    score: number(raw.match_score ?? raw.score_composto ?? raw.score_cruzamento),
    scoreTexto: number(raw.score_texto), scoreValor: number(raw.score_valor), scoreData: number(raw.score_data),
    diferencaValorPercentual: number(raw.diferenca_valor_percentual ?? raw.diff_valor_percent),
    diferencaDataDias: number(raw.diferenca_data_dias),
    estrategia: text(raw.estrategia_match || raw.estrategia),
  };
}

function normalizeData(data: NonNullable<JobStatus['data']>): CrossmatchData {
  return {
    ambos: (data.ambos || []).map((r, i) => normalize(r, 'ambos', i)),
    apenas_pncp: (data.apenas_pncp || []).map((r, i) => normalize(r, 'apenas_pncp', i)),
    apenas_aplic: (data.apenas_aplic || []).map((r, i) => normalize(r, 'apenas_aplic', i)),
  };
}

async function readJson<T>(response: Response): Promise<T> {
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(body.message || body.error || `Erro HTTP ${response.status}`);
  return body as T;
}

export async function runCrossmatch(
  municipio: string,
  ano: string,
  force: boolean,
  onProgress: (progress: JobProgress) => void,
  signal: AbortSignal,
): Promise<CrossmatchData> {
  const query = new URLSearchParams({ municipio, ano, force: String(force) });
  const started = await readJson<{ jobId: string }>(await fetch(`${API_BASE}/start?${query}`, { signal }));

  while (!signal.aborted) {
    const job = await readJson<JobStatus>(await fetch(`${API_BASE}/status?jobId=${encodeURIComponent(started.jobId)}`, { signal }));
    onProgress(job);
    if (job.status === 'error') throw createJobError(job);
    if (job.status === 'done' && job.data) return normalizeData(job.data);
    await new Promise<void>((resolve, reject) => {
      const timer = window.setTimeout(resolve, 1000);
      signal.addEventListener('abort', () => { window.clearTimeout(timer); reject(new DOMException('Abortado', 'AbortError')); }, { once: true });
    });
  }
  throw new DOMException('Abortado', 'AbortError');
}

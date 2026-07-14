export type Bucket = 'ambos' | 'apenas_pncp' | 'apenas_aplic';

export interface Licitacao {
  id: string;
  orgao: string;
  modalidade: string;
  numero: string;
  ano: string;
  objeto: string;
  valor: number;
  cnpj: string;
  numeroControlePNCP?: string;
  dataPNCP?: string;
  dataAPLIC?: string;
  prazoAplic?: string;
  alertaAtivo?: boolean;
  statusPrazo?: 'vencido' | 'vence_hoje' | 'aguardando' | 'sem_data' | 'nao_aplicavel';
  diasUteisRestantes?: number;
  orgaoAplic?: string;
  numeroAplic?: string;
  objetoAplic?: string;
  valorAplic?: number;
  score?: number;
  scoreTexto?: number;
  scoreValor?: number;
  scoreData?: number;
  diferencaValorPercentual?: number;
  diferencaDataDias?: number;
  estrategia?: string;
}

export interface CrossmatchData {
  ambos: Licitacao[];
  apenas_pncp: Licitacao[];
  apenas_aplic: Licitacao[];
}

export interface JobProgress {
  status: 'idle' | 'running' | 'done' | 'error';
  stage: string;
  message: string;
  current: number;
  total: number;
  cachedAt?: string;
  errorCode?: string;
  errorHint?: string;
}

export interface RunMeta {
  municipio: string;
  ano: string;
  startedAt: string;
  finishedAt?: string;
  status: 'running' | 'done' | 'error';
  cachedAt?: string;
}

import { useMemo, useState } from 'react';
import type { MouseEvent, ReactNode } from 'react';
import {
  AlertTriangle,
  CheckCircle2,
  Copy,
  ExternalLink,
  FileWarning,
  Play,
  RefreshCw,
  Scale,
  ServerCog,
} from 'lucide-react';
import { useCrossmatch } from '../hooks/useCrossmatch';
import type { Bucket, Licitacao, RunMeta } from '../types';
import { AuditDrawer, AuditPanel } from './AuditDrawer';
import { Filters, type FilterState } from './Filters';
import { ProgressPanel } from './ProgressPanel';

const initialFilters: FilterState = { search: '', orgao: '', modalidade: '', prazo: '', minScore: '' };
const money = (value: number) => new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL', maximumFractionDigits: 0 }).format(value);
const date = (value?: string) => value ? new Date(`${value}T12:00:00`).toLocaleDateString('pt-BR') : '—';
const dateTime = (value?: string) => value ? new Date(value).toLocaleString('pt-BR') : '—';

export default function Dashboard() {
  const [municipio, setMunicipio] = useState('Sinop');
  const [ano, setAno] = useState('2026');
  const [activeTab, setActiveTab] = useState<Bucket>('ambos');
  const [filters, setFilters] = useState(initialFilters);
  const [selected, setSelected] = useState<Licitacao | null>(null);
  const [copied, setCopied] = useState('');
  const { data, progress, error, lastRun, execute } = useCrossmatch();
  const running = progress.status === 'running';

  const current = data[activeTab];
  const orgaos = useMemo(() => unique(current.map((x) => x.orgao)), [current]);
  const modalidades = useMemo(() => unique(current.map((x) => x.modalidade)), [current]);
  const filtered = useMemo(() => sortItems(
    current.filter((item) => {
      const query = filters.search.toLocaleLowerCase('pt-BR').trim();
      const searchable = `${item.numero} ${item.ano} ${item.orgao} ${item.objeto} ${item.cnpj}`.toLocaleLowerCase('pt-BR');
      const scorePass = filters.minScore === '' || (filters.minScore === '0' ? (item.score || 0) < 60 : (item.score || 0) >= Number(filters.minScore));

      return (!query || searchable.includes(query))
        && (!filters.orgao || item.orgao === filters.orgao)
        && (!filters.modalidade || item.modalidade === filters.modalidade)
        && (!filters.prazo || item.statusPrazo === filters.prazo)
        && scorePass;
    }),
    activeTab,
  ), [activeTab, current, filters]);

  const totals = useMemo(() => ({
    ambos: summarize(data.ambos),
    pncp: summarize(data.apenas_pncp),
    aplic: summarize(data.apenas_aplic),
    vencidos: data.apenas_pncp.filter((x) => x.statusPrazo === 'vencido').length,
  }), [data]);

  const sourceStatus = useMemo(() => buildSourceStatus(progress, lastRun), [lastRun, progress]);

  const submit = (force = false) => {
    if (!municipio.trim()) return;
    setSelected(null);
    setFilters(initialFilters);
    execute(municipio.trim(), ano, force);
  };

  const copyText = async (label: string, value: string) => {
    if (!value) return;
    await navigator.clipboard.writeText(value);
    setCopied(label);
    window.setTimeout(() => setCopied(''), 1600);
  };

  return (
    <div className="min-h-screen bg-slate-100 text-slate-800">
      <header className="border-b border-blue-800 bg-blue-950 text-white">
        <div className="mx-auto flex max-w-[1500px] flex-col gap-4 px-5 py-5">
          <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
            <div className="flex items-center gap-3">
              <div className="rounded-xl bg-blue-800 p-2">
                <Scale />
              </div>
              <div>
                <h1 className="text-xl font-bold">Monitor PNCP x APLIC</h1>
                <p className="text-sm text-blue-200">Analise, rastreio operacional e auditoria de contratacoes publicas</p>
              </div>
            </div>

            <div className="flex flex-wrap gap-2">
              <input value={municipio} onChange={(e) => setMunicipio(e.target.value)} placeholder="Municipio" className="min-w-52 rounded-lg border border-blue-700 bg-blue-900 px-3 py-2 text-sm text-white placeholder:text-blue-300" />
              <select value={ano} onChange={(e) => setAno(e.target.value)} className="rounded-lg border border-blue-700 bg-blue-900 px-3 py-2 text-sm">
                {[2024, 2025, 2026].map((x) => <option key={x}>{x}</option>)}
              </select>
              <button onClick={() => submit(false)} disabled={running || !municipio.trim()} className="inline-flex items-center gap-2 rounded-lg bg-white px-4 py-2 text-sm font-bold text-blue-900 disabled:opacity-50">
                <Play className="h-4 w-4" /> Analisar
              </button>
              <button onClick={() => submit(true)} disabled={running || !municipio.trim()} title="Ignorar cache e consultar novamente" className="rounded-lg border border-blue-600 p-2 disabled:opacity-50">
                <RefreshCw className="h-5 w-5" />
              </button>
            </div>
          </div>

          <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
            <div className="flex flex-wrap gap-2">
              {sourceStatus.map((item) => <SourceBadge key={item.label} {...item} />)}
            </div>
            <RunSummary lastRun={lastRun} />
          </div>
        </div>
      </header>

      <main className="mx-auto max-w-[1500px] p-5">
        <ProgressPanel progress={progress} error={error} />

        <div className="mb-5 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
          <Kpi title="Cruzadas" count={totals.ambos.count} value={totals.ambos.value} color="emerald" icon={<CheckCircle2 />} />
          <Kpi title="Falta no APLIC" count={totals.pncp.count} value={totals.pncp.value} color="amber" icon={<AlertTriangle />} detail={`${totals.vencidos} vencidas`} />
          <Kpi title="Falta no PNCP" count={totals.aplic.count} value={totals.aplic.value} color="orange" icon={<FileWarning />} />
          <Kpi title="Cobertura" count={totals.ambos.count + totals.pncp.count + totals.aplic.count} value={coverage(totals)} color="blue" icon={<Scale />} isPercent />
        </div>

        <div className="flex gap-5">
          <section className="min-w-0 flex-1 overflow-hidden rounded-xl border border-slate-200 bg-white shadow-sm">
            <div className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-200 px-4 pt-3">
              <div className="flex overflow-x-auto">
                <Tab active={activeTab === 'ambos'} onClick={() => setActiveTab('ambos')} label="Cruzadas" count={data.ambos.length} />
                <Tab active={activeTab === 'apenas_pncp'} onClick={() => setActiveTab('apenas_pncp')} label="Falta no APLIC" count={data.apenas_pncp.length} />
                <Tab active={activeTab === 'apenas_aplic'} onClick={() => setActiveTab('apenas_aplic')} label="Falta no PNCP" count={data.apenas_aplic.length} />
              </div>
              {activeTab === 'ambos' && (
                <div className="mb-3 flex flex-wrap gap-2 text-xs">
                  <Legend label="Alta confianca" tone="emerald" />
                  <Legend label="Revisar" tone="amber" />
                  <Legend label="Risco alto" tone="rose" />
                </div>
              )}
            </div>

            <Filters value={filters} onChange={setFilters} orgaos={orgaos} modalidades={modalidades} showMatch={activeTab === 'ambos'} showPrazo={activeTab === 'apenas_pncp'} />

            <div className="flex items-center justify-between border-b border-slate-100 px-4 py-2 text-xs text-slate-500">
              <span>{filtered.length} de {current.length} registros</span>
              <span>{copied ? `${copied} copiado` : 'Clique na linha para auditar'}</span>
            </div>

            <ResultsTable
              items={filtered}
              bucket={activeTab}
              selectedId={selected?.id}
              onSelect={setSelected}
              onCopy={copyText}
              onResetFilters={() => setFilters(initialFilters)}
            />
          </section>

          <AuditPanel item={selected} bucket={activeTab} onClose={() => setSelected(null)} />
        </div>
      </main>

      <AuditDrawer item={selected} bucket={activeTab} onClose={() => setSelected(null)} />
    </div>
  );
}

function ResultsTable({
  items,
  bucket,
  selectedId,
  onSelect,
  onCopy,
  onResetFilters,
}: {
  items: Licitacao[];
  bucket: Bucket;
  selectedId?: string;
  onSelect: (item: Licitacao) => void;
  onCopy: (label: string, value: string) => void;
  onResetFilters: () => void;
}) {
  if (!items.length) {
    return (
      <div className="p-14 text-center text-slate-500">
        <div className="mx-auto max-w-md space-y-3">
          <p className="text-base font-semibold">Nenhum registro encontrado neste recorte.</p>
          <p className="text-sm text-slate-400">Isso pode acontecer por filtros ativos, ausencia de dados na fonte ou nenhum resultado para o municipio consultado.</p>
          <button onClick={onResetFilters} className="rounded-lg border border-slate-300 px-3 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50">
            Limpar filtros
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="max-h-[58vh] overflow-auto">
      <table className="w-full min-w-[1120px] text-left text-sm">
        <thead className="sticky top-0 bg-slate-50 text-xs uppercase text-slate-500">
          <tr>
            <th className="p-3">Numero/ano</th>
            <th className="p-3">Orgao</th>
            <th className="p-3">Modalidade</th>
            <th className="p-3">Objeto</th>
            <th className="p-3 text-right">Valor</th>
            <th className="p-3">{bucket === 'ambos' ? 'Score' : bucket === 'apenas_pncp' ? 'Prazo' : 'Situacao'}</th>
            <th className="p-3 text-right">Acoes</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {items.map((item) => (
            <tr
              key={item.id}
              onClick={() => onSelect(item)}
              tabIndex={0}
              onKeyDown={(e) => e.key === 'Enter' && onSelect(item)}
              className={`group cursor-pointer hover:bg-blue-50 ${selectedId === item.id ? 'bg-blue-50/80 ring-1 ring-inset ring-blue-200' : ''}`}
            >
              <td className="whitespace-nowrap p-3 font-mono">{item.numero}/{item.ano}</td>
              <td className="max-w-56 truncate p-3 font-medium" title={item.orgao}>{item.orgao || '—'}</td>
              <td className="p-3 text-slate-600">{item.modalidade || '—'}</td>
              <td className="max-w-lg p-3">
                <div className="line-clamp-2" title={item.objeto}>{item.objeto || '—'}</div>
              </td>
              <td className="whitespace-nowrap p-3 text-right font-medium">{money(item.valor)}</td>
              <td className="p-3"><Status item={item} bucket={bucket} /></td>
              <td className="p-3">
                <div className="flex justify-end gap-1 opacity-100 lg:opacity-0 lg:transition group-hover:opacity-100">
                  <ActionButton label="Copiar numero" onClick={(e) => { e.stopPropagation(); void onCopy('Numero', `${item.numero}/${item.ano}`); }}>
                    <Copy className="h-4 w-4" />
                  </ActionButton>
                  <ActionButton label="Copiar objeto" onClick={(e) => { e.stopPropagation(); void onCopy('Objeto', item.objeto); }}>
                    <ServerCog className="h-4 w-4" />
                  </ActionButton>
                  {item.numeroControlePNCP && (
                    <ActionButton
                      label="Abrir no PNCP"
                      onClick={(e) => {
                        e.stopPropagation();
                        window.open(`https://pncp.gov.br/app/editais?id=${encodeURIComponent(item.numeroControlePNCP || '')}`, '_blank', 'noopener,noreferrer');
                      }}
                    >
                      <ExternalLink className="h-4 w-4" />
                    </ActionButton>
                  )}
                </div>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function ActionButton({ children, label, onClick }: { children: ReactNode; label: string; onClick: (event: MouseEvent<HTMLButtonElement>) => void }) {
  return (
    <button onClick={onClick} title={label} className="rounded-md border border-slate-200 p-2 text-slate-500 hover:border-blue-200 hover:bg-white hover:text-blue-700">
      {children}
    </button>
  );
}

function Status({ item, bucket }: { item: Licitacao; bucket: Bucket }) {
  if (bucket === 'ambos') {
    const tone = scoreTone(item.score || 0);
    return <span className={`rounded-full px-2 py-1 text-xs font-bold ${tone.className}`}>{Math.round(item.score || 0)}%</span>;
  }

  if (bucket === 'apenas_aplic') {
    return <span className="rounded-full bg-orange-100 px-2 py-1 text-xs font-bold text-orange-700">Sem PNCP</span>;
  }

  const labels = {
    vencido: 'Vencido',
    vence_hoje: 'Vence hoje',
    aguardando: `Ate ${date(item.prazoAplic)}`,
    sem_data: 'Sem data',
    nao_aplicavel: '—',
  };

  const status = item.statusPrazo || 'sem_data';
  const tone = status === 'vencido' ? 'bg-rose-100 text-rose-700' : status === 'vence_hoje' ? 'bg-orange-100 text-orange-700' : 'bg-amber-100 text-amber-700';
  return <span className={`rounded-full px-2 py-1 text-xs font-bold ${tone}`}>{labels[status]}</span>;
}

function RunSummary({ lastRun }: { lastRun: RunMeta | null }) {
  if (!lastRun) return <p className="text-sm text-blue-200">Nenhuma analise executada nesta sessao.</p>;

  return (
    <div className="text-sm text-blue-200">
      <span className="font-semibold text-white">{lastRun.municipio}/{lastRun.ano}</span>
      {' · '}
      <span>{lastRun.status === 'running' ? 'em andamento' : lastRun.status === 'done' ? 'concluida' : 'com erro'}</span>
      {' · '}
      <span>{dateTime(lastRun.finishedAt || lastRun.startedAt)}</span>
      {lastRun.cachedAt && <span>{' · cache de '}{dateTime(lastRun.cachedAt)}</span>}
    </div>
  );
}

function SourceBadge({ label, value, tone }: { label: string; value: string; tone: 'emerald' | 'amber' | 'rose' | 'blue' }) {
  const tones = {
    emerald: 'border-emerald-200 bg-emerald-50 text-emerald-700',
    amber: 'border-amber-200 bg-amber-50 text-amber-700',
    rose: 'border-rose-200 bg-rose-50 text-rose-700',
    blue: 'border-blue-200 bg-blue-50 text-blue-700',
  } as const;

  return (
    <div className={`rounded-full border px-3 py-1 text-xs font-medium ${tones[tone]}`}>
      <span className="mr-1 opacity-70">{label}:</span>
      <span>{value}</span>
    </div>
  );
}

function Legend({ label, tone }: { label: string; tone: 'emerald' | 'amber' | 'rose' }) {
  const color = scoreLegendClass(tone);
  return <span className={`rounded-full px-2 py-1 font-medium ${color}`}>{label}</span>;
}

function buildSourceStatus(progress: { status: string; stage: string; errorCode?: string }, lastRun: RunMeta | null) {
  const oracleDown = progress.errorCode?.startsWith('oracle_');

  return [
    { label: 'API', value: 'Online', tone: 'emerald' as const },
    {
      label: 'Oracle',
      value: oracleDown ? 'Indisponivel' : lastRun?.status === 'done' ? 'Consultado' : progress.stage === 'ugs' || progress.stage === 'aplic' ? 'Consultando' : 'Aguardando',
      tone: oracleDown ? 'rose' as const : lastRun?.status === 'done' ? 'emerald' as const : 'blue' as const,
    },
    {
      label: 'PNCP',
      value: progress.stage === 'pncp' ? 'Consultando' : lastRun?.status === 'done' && !oracleDown ? 'Consultado' : 'Aguardando',
      tone: progress.stage === 'pncp' ? 'amber' as const : lastRun?.status === 'done' && !oracleDown ? 'emerald' as const : 'blue' as const,
    },
    {
      label: 'Cache',
      value: lastRun?.cachedAt ? `Usado em ${dateTime(lastRun.cachedAt)}` : 'Sem reaproveitamento',
      tone: lastRun?.cachedAt ? 'amber' as const : 'blue' as const,
    },
  ];
}

const kpiColors = {
  emerald: ['border-emerald-500', 'text-emerald-500'],
  amber: ['border-amber-500', 'text-amber-500'],
  orange: ['border-orange-500', 'text-orange-500'],
  blue: ['border-blue-500', 'text-blue-500'],
} as const;

function Kpi({ title, count, value, color, icon, detail, isPercent }: { title: string; count: number; value: number; color: keyof typeof kpiColors; icon: ReactNode; detail?: string; isPercent?: boolean }) {
  const colors = kpiColors[color];
  return (
    <div className={`rounded-xl border-l-4 ${colors[0]} bg-white p-4 shadow-sm`}>
      <div className="flex justify-between">
        <div>
          <p className="text-xs font-bold uppercase tracking-wider text-slate-400">{title}</p>
          <p className="mt-1 text-2xl font-bold">{isPercent ? `${value}%` : money(value)}</p>
          <p className="text-sm text-slate-500">{count} registros{detail ? ` · ${detail}` : ''}</p>
        </div>
        <div className={colors[1]}>{icon}</div>
      </div>
    </div>
  );
}

function Tab({ active, onClick, label, count }: { active: boolean; onClick: () => void; label: string; count: number }) {
  return <button onClick={onClick} className={`whitespace-nowrap border-b-2 px-5 py-3 text-sm font-semibold ${active ? 'border-blue-600 text-blue-700' : 'border-transparent text-slate-500'}`}>{label} <span className="ml-1 rounded-full bg-slate-100 px-2 py-0.5 text-xs">{count}</span></button>;
}

function summarize(items: Licitacao[]) {
  return { count: items.length, value: items.reduce((sum, item) => sum + item.valor, 0) };
}

function unique(values: string[]) {
  return [...new Set(values.filter(Boolean))].sort((a, b) => a.localeCompare(b, 'pt-BR'));
}

function coverage(t: { ambos: { count: number }; pncp: { count: number }; aplic: { count: number } }) {
  const total = t.ambos.count + t.pncp.count + t.aplic.count;
  return total ? Math.round((t.ambos.count / total) * 100) : 0;
}

function sortItems(items: Licitacao[], bucket: Bucket) {
  return [...items].sort((a, b) => {
    if (bucket === 'ambos') return (a.score || 0) - (b.score || 0);
    if (bucket === 'apenas_pncp') {
      const order = { vencido: 0, vence_hoje: 1, aguardando: 2, sem_data: 3, nao_aplicavel: 4 } as const;
      const left = order[a.statusPrazo || 'sem_data'];
      const right = order[b.statusPrazo || 'sem_data'];
      if (left !== right) return left - right;
      return (a.diasUteisRestantes ?? 999) - (b.diasUteisRestantes ?? 999);
    }
    return b.valor - a.valor;
  });
}

function scoreTone(score: number) {
  if (score >= 85) return { className: scoreLegendClass('emerald') };
  if (score >= 70) return { className: scoreLegendClass('amber') };
  return { className: scoreLegendClass('rose') };
}

function scoreLegendClass(tone: 'emerald' | 'amber' | 'rose') {
  if (tone === 'emerald') return 'bg-emerald-100 text-emerald-700';
  if (tone === 'amber') return 'bg-amber-100 text-amber-700';
  return 'bg-rose-100 text-rose-700';
}

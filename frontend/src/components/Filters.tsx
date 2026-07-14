import { Search } from 'lucide-react';

export interface FilterState {
  search: string;
  orgao: string;
  modalidade: string;
  prazo: string;
  minScore: string;
}

export function Filters({ value, onChange, orgaos, modalidades, showMatch, showPrazo }: {
  value: FilterState;
  onChange: (next: FilterState) => void;
  orgaos: string[];
  modalidades: string[];
  showMatch: boolean;
  showPrazo: boolean;
}) {
  const set = (key: keyof FilterState, next: string) => onChange({ ...value, [key]: next });
  return (
    <div className="grid gap-3 border-b border-slate-200 bg-white p-4 md:grid-cols-2 xl:grid-cols-5">
      <label className="relative xl:col-span-2">
        <span className="sr-only">Buscar</span><Search className="absolute left-3 top-2.5 h-4 w-4 text-slate-400" />
        <input value={value.search} onChange={e => set('search', e.target.value)} placeholder="Buscar número, órgão ou objeto" className="w-full rounded-lg border border-slate-300 py-2 pl-9 pr-3 text-sm outline-none focus:border-blue-500" />
      </label>
      <select value={value.orgao} onChange={e => set('orgao', e.target.value)} className="rounded-lg border border-slate-300 px-3 py-2 text-sm"><option value="">Todos os órgãos</option>{orgaos.map(x => <option key={x}>{x}</option>)}</select>
      <select value={value.modalidade} onChange={e => set('modalidade', e.target.value)} className="rounded-lg border border-slate-300 px-3 py-2 text-sm"><option value="">Todas as modalidades</option>{modalidades.map(x => <option key={x}>{x}</option>)}</select>
      {showPrazo && <select value={value.prazo} onChange={e => set('prazo', e.target.value)} className="rounded-lg border border-slate-300 px-3 py-2 text-sm"><option value="">Todos os prazos</option><option value="vencido">Vencidos</option><option value="vence_hoje">Vencem hoje</option><option value="aguardando">Dentro do prazo</option><option value="sem_data">Sem data</option></select>}
      {showMatch && <select value={value.minScore} onChange={e => set('minScore', e.target.value)} className="rounded-lg border border-slate-300 px-3 py-2 text-sm"><option value="">Qualquer score</option><option value="80">Score ≥ 80</option><option value="60">Score ≥ 60</option><option value="0">Baixa confiança</option></select>}
    </div>
  );
}

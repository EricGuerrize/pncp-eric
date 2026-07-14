import { PanelRightClose, X } from 'lucide-react';
import type { Bucket, Licitacao } from '../types';

const money = (value?: number) => value == null ? '—' : new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(value);
const date = (value?: string) => value ? new Date(`${value}T12:00:00`).toLocaleDateString('pt-BR') : '—';

const metric = (label: string, value?: number, suffix = '%') => (
  <div className="rounded-lg bg-slate-50 p-3">
    <span className="block text-xs uppercase text-slate-400">{label}</span>
    <strong>{value == null ? 'Nao informado' : `${value.toFixed(1)}${suffix}`}</strong>
  </div>
);

export function AuditDrawer({ item, bucket, onClose }: { item: Licitacao | null; bucket: Bucket; onClose: () => void }) {
  if (!item) return null;

  return (
    <div className="fixed inset-0 z-50 flex justify-end bg-slate-950/40 xl:hidden" onMouseDown={onClose}>
      <aside className="h-full w-full max-w-3xl overflow-y-auto bg-white p-6 shadow-2xl" onMouseDown={(e) => e.stopPropagation()} aria-label="Detalhes do cruzamento">
        <AuditContent item={item} bucket={bucket} onClose={onClose} />
      </aside>
    </div>
  );
}

export function AuditPanel({ item, bucket, onClose }: { item: Licitacao | null; bucket: Bucket; onClose: () => void }) {
  return (
    <aside className="hidden xl:block xl:w-[420px] xl:shrink-0">
      <div className="sticky top-5 overflow-hidden rounded-xl border border-slate-200 bg-white shadow-sm">
        {item ? (
          <div className="max-h-[calc(100vh-3rem)] overflow-y-auto p-6">
            <AuditContent item={item} bucket={bucket} onClose={onClose} compact />
          </div>
        ) : (
          <div className="flex min-h-[420px] flex-col items-center justify-center gap-4 p-8 text-center text-slate-400">
            <PanelRightClose className="h-10 w-10 text-slate-300" />
            <div>
              <p className="text-base font-semibold text-slate-500">Nenhum registro selecionado</p>
              <p className="mt-1 text-sm">Clique em uma linha da tabela para abrir a auditoria fixa ao lado.</p>
            </div>
          </div>
        )}
      </div>
    </aside>
  );
}

function AuditContent({ item, bucket, onClose, compact = false }: { item: Licitacao; bucket: Bucket; onClose: () => void; compact?: boolean }) {
  return (
    <>
      <div className="mb-6 flex items-start justify-between gap-4">
        <div>
          <p className="text-xs font-bold uppercase tracking-wider text-blue-600">Auditoria do registro</p>
          <h2 className="text-xl font-bold text-slate-900">{item.numero}/{item.ano}</h2>
        </div>
        <button onClick={onClose} className="rounded-lg p-2 hover:bg-slate-100" aria-label="Fechar selecao">
          <X />
        </button>
      </div>

      <div className={`grid gap-4 ${compact ? '' : 'md:grid-cols-2'}`}>
        <Side title="PNCP" orgao={item.orgao} objeto={item.objeto} valor={item.valor} data={date(item.dataPNCP)} />
        <Side
          title="APLIC"
          orgao={item.orgaoAplic || (bucket === 'apenas_aplic' ? item.orgao : '')}
          objeto={item.objetoAplic || (bucket === 'apenas_aplic' ? item.objeto : '')}
          valor={item.valorAplic ?? (bucket === 'apenas_aplic' ? item.valor : undefined)}
          data={date(item.dataAPLIC)}
        />
      </div>

      {bucket === 'ambos' && (
        <>
          <h3 className="mb-3 mt-7 font-bold">Como o sistema chegou a este resultado</h3>
          <div className={`grid gap-3 ${compact ? 'grid-cols-2' : 'grid-cols-2 md:grid-cols-4'}`}>
            {metric('Score final', item.score)}
            {metric('Texto', item.scoreTexto)}
            {metric('Valor', item.scoreValor)}
            {metric('Data', item.scoreData)}
          </div>
          <div className="mt-4 rounded-lg border border-slate-200 p-4 text-sm">
            <strong>Estrategia:</strong> {item.estrategia || 'Nao informada'}
            <br />
            <span className="text-slate-500">
              Diferenca de valor: {item.diferencaValorPercentual?.toFixed(1) || '—'}% · Diferenca de data: {item.diferencaDataDias?.toFixed(0) || '—'} dias
            </span>
          </div>
        </>
      )}

      {bucket === 'apenas_pncp' && (
        <div className="mt-6 rounded-lg border border-amber-200 bg-amber-50 p-4">
          <strong>{item.statusPrazo === 'vencido' ? 'Prazo vencido' : item.statusPrazo === 'vence_hoje' ? 'Vence hoje' : 'Aguardando APLIC'}</strong>
          <p className="mt-1 text-sm text-slate-600">Publicacao: {date(item.dataPNCP)} · Prazo calculado: {date(item.prazoAplic)}</p>
        </div>
      )}
    </>
  );
}

function Side({ title, orgao, objeto, valor, data }: { title: string; orgao?: string; objeto?: string; valor?: number; data: string }) {
  return (
    <section className="rounded-xl border border-slate-200 p-4">
      <h3 className="mb-4 font-bold text-blue-700">{title}</h3>
      <dl className="space-y-3 text-sm">
        <div>
          <dt className="text-xs uppercase text-slate-400">Orgao</dt>
          <dd>{orgao || '—'}</dd>
        </div>
        <div>
          <dt className="text-xs uppercase text-slate-400">Objeto</dt>
          <dd className="whitespace-pre-wrap">{objeto || '—'}</dd>
        </div>
        <div>
          <dt className="text-xs uppercase text-slate-400">Valor</dt>
          <dd>{money(valor)}</dd>
        </div>
        <div>
          <dt className="text-xs uppercase text-slate-400">Data</dt>
          <dd>{data}</dd>
        </div>
      </dl>
    </section>
  );
}

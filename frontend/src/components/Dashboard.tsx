import { useState, useEffect } from 'react';
import { db } from '../firebase';
import { collection, getDocs, onSnapshot } from 'firebase/firestore';
import { 
  BarChart3, 
  AlertTriangle, 
  CheckCircle2, 
  Clock, 
  FileWarning, 
  MapPin,
  RefreshCw
} from 'lucide-react';

interface Licitacao {
  id: string;
  municipio: string;
  orgao: string;
  modalidade: string;
  numero: string;
  ano: string;
  objeto: string;
  valor: number | null;
  cnpj: string;
  statusPNCP: string;
  statusAPLIC: string;
  alertaAtivo?: boolean;
  score_cruzamento?: string;
  dataPNCP?: any;
  dataAPLIC?: any;
}

export default function Dashboard() {
  const [activeTab, setActiveTab] = useState<'ambos' | 'apenas_pncp' | 'apenas_aplic'>('ambos');
  const [municipios, setMunicipios] = useState<{id: string, nome: string}[]>([]);
  const [selectedMun, setSelectedMun] = useState<string>('sinop');
  
  const [data, setData] = useState<Licitacao[]>([]);
  const [loading, setLoading] = useState(true);

  // Stats
  const [kpis, setKpis] = useState({
    ambos: 0,
    pncp: 0,
    aplic: 0,
    alertas: 0
  });

  // Load municipalities on mount
  useEffect(() => {
    const fetchMunicipios = async () => {
      try {
        const snap = await getDocs(collection(db, 'municipios'));
        const muns: any[] = [];
        snap.forEach(doc => {
          muns.push({ id: doc.id, nome: doc.data().nome || doc.id });
        });
        if (muns.length > 0) {
          setMunicipios(muns);
          if (!muns.find(m => m.id === selectedMun)) setSelectedMun(muns[0].id);
        } else {
          // Fallback if empty db
          setMunicipios([{ id: 'sinop', nome: 'Sinop' }]);
        }
      } catch (err) {
        console.error("Failed to fetch municipalities", err);
      }
    };
    fetchMunicipios();
  }, [selectedMun]);

  // Subscribe to real-time changes
  useEffect(() => {
    if (!selectedMun) return;
    setLoading(true);

    const unsubAmbos = onSnapshot(collection(db, `municipios/${selectedMun}/ambos`), (snap) => {
      const items = snap.docs.map(d => ({ id: d.id, ...d.data() } as Licitacao));
      setKpis(prev => ({ ...prev, ambos: items.length }));
      if (activeTab === 'ambos') { setData(items); setLoading(false); }
    });

    const unsubPncp = onSnapshot(collection(db, `municipios/${selectedMun}/apenas_pncp`), (snap) => {
      const items = snap.docs.map(d => ({ id: d.id, ...d.data() } as Licitacao));
      const alertas = items.filter(i => i.alertaAtivo).length;
      setKpis(prev => ({ ...prev, pncp: items.length, alertas }));
      if (activeTab === 'apenas_pncp') { setData(items); setLoading(false); }
    });

    const unsubAplic = onSnapshot(collection(db, `municipios/${selectedMun}/apenas_aplic`), (snap) => {
      const items = snap.docs.map(d => ({ id: d.id, ...d.data() } as Licitacao));
      setKpis(prev => ({ ...prev, aplic: items.length }));
      if (activeTab === 'apenas_aplic') { setData(items); setLoading(false); }
    });

    return () => {
      unsubAmbos();
      unsubPncp();
      unsubAplic();
    };
  }, [selectedMun, activeTab]);

  const formatCurrency = (val: number | null) => {
    if (val === null || val === undefined) return '—';
    return new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(val);
  };

  return (
    <div className="min-h-screen flex flex-col w-full text-slate-800 font-sans">
      {/* Header */}
      <header className="bg-primary-900 text-white px-6 py-4 flex items-center justify-between shadow-md">
        <div className="flex items-center gap-3">
          <BarChart3 className="w-6 h-6 text-primary-100" />
          <div>
            <h1 className="text-lg font-bold tracking-wide">Monitor PNCP × APLIC</h1>
            <p className="text-xs text-primary-100 opacity-80 mt-0.5">TCE-MT Audit Dashboard</p>
          </div>
        </div>
        <div className="flex items-center gap-4">
          <div className="flex bg-primary-800 rounded-md p-1 border border-primary-600/50">
            <MapPin className="w-4 h-4 text-primary-100 m-1" />
            <select 
              value={selectedMun} 
              onChange={e => setSelectedMun(e.target.value)}
              className="bg-transparent text-sm text-white focus:outline-none pr-2 appearance-none cursor-pointer"
            >
              {municipios.map(m => (
                <option key={m.id} value={m.id} className="text-slate-900">{m.nome}</option>
              ))}
            </select>
          </div>
        </div>
      </header>

      <main className="flex-1 p-6 max-w-7xl mx-auto w-full">
        {/* KPI Cards */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-8">
          <KPICard 
            title="Em Ambos" 
            value={kpis.ambos} 
            desc="PNCP + APLIC" 
            icon={<CheckCircle2 className="w-5 h-5 text-emerald-500" />} 
            color="border-l-emerald-500"
          />
          <KPICard 
            title="Apenas PNCP" 
            value={kpis.pncp} 
            desc="Aguardando APLIC" 
            icon={<Clock className="w-5 h-5 text-amber-500" />} 
            color="border-l-amber-500"
          />
          <KPICard 
            title="Apenas APLIC" 
            value={kpis.aplic} 
            desc="Sem PNCP (Falha Grave)" 
            icon={<FileWarning className="w-5 h-5 text-orange-500" />} 
            color="border-l-orange-500"
          />
          <KPICard 
            title="Alertas (Prazos)" 
            value={kpis.alertas} 
            desc="Prazo 3 dias vencido!" 
            icon={<AlertTriangle className="w-5 h-5 text-rose-500" />} 
            color="border-l-rose-500"
            isAlert={kpis.alertas > 0}
          />
        </div>

        {/* Space-efficient Tabs */}
        <div className="bg-white rounded-t-lg border-b border-slate-200 px-4 pt-3 flex gap-2">
          <TabButton 
            active={activeTab === 'ambos'} 
            onClick={() => setActiveTab('ambos')}
            label="100% Sincronizados"
            count={kpis.ambos}
          />
          <TabButton 
            active={activeTab === 'apenas_pncp'} 
            onClick={() => setActiveTab('apenas_pncp')}
            label="Apenas PNCP"
            count={kpis.pncp}
          />
          <TabButton 
            active={activeTab === 'apenas_aplic'} 
            onClick={() => setActiveTab('apenas_aplic')}
            label="Apenas APLIC"
            count={kpis.aplic}
          />
        </div>

        {/* Data Table */}
        <div className="bg-white rounded-b-lg border border-t-0 border-slate-200 overflow-hidden shadow-sm">
          {loading ? (
            <div className="p-12 text-center text-slate-400 flex flex-col items-center">
              <RefreshCw className="w-8 h-8 animate-spin mb-3 text-primary-500" />
              <p>Sincronizando com Firestore...</p>
            </div>
          ) : data.length === 0 ? (
            <div className="p-12 text-center text-slate-400">
              <p>Nenhuma licitação encontrada nesta categoria.</p>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-left text-sm whitespace-nowrap">
                <thead className="bg-slate-50 text-slate-600 border-b border-slate-200">
                  <tr>
                    <th className="py-3 px-4 font-semibold text-xs tracking-wider uppercase">Número/Ano</th>
                    <th className="py-3 px-4 font-semibold text-xs tracking-wider uppercase">Órgão</th>
                    <th className="py-3 px-4 font-semibold text-xs tracking-wider uppercase">Modalidade</th>
                    <th className="py-3 px-4 font-semibold text-xs tracking-wider uppercase w-1/3">Objeto</th>
                    <th className="py-3 px-4 font-semibold text-xs tracking-wider uppercase text-right">Valor Estimado</th>
                    <th className="py-3 px-4 font-semibold text-xs tracking-wider uppercase">Status</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100">
                  {data.map((item) => (
                    <tr key={item.id} className={`hover:bg-slate-50 transition-colors ${item.alertaAtivo ? 'bg-rose-50/50' : ''}`}>
                      <td className="py-3 px-4 font-mono text-xs">{item.numero}/{item.ano}</td>
                      <td className="py-3 px-4 font-medium text-slate-700 truncate max-w-[200px]" title={item.orgao}>{item.orgao}</td>
                      <td className="py-3 px-4 text-slate-600">{item.modalidade}</td>
                      <td className="py-3 px-4">
                        <div className="truncate max-w-[300px] text-slate-600" title={item.objeto}>
                          {item.objeto}
                        </div>
                      </td>
                      <td className="py-3 px-4 text-right font-medium text-slate-700">
                        {formatCurrency(item.valor)}
                      </td>
                      <td className="py-3 px-4">
                        <div className="flex flex-col gap-1 items-start">
                          <StatusBadge type={activeTab} />
                          {item.alertaAtivo && (
                            <span className="inline-flex items-center gap-1 text-[10px] uppercase tracking-wider font-bold text-rose-600 bg-rose-100 px-1.5 py-0.5 rounded">
                              <AlertTriangle className="w-3 h-3" /> Vencido
                            </span>
                          )}
                          {item.score_cruzamento && (
                            <span className="text-[10px] text-slate-500" title="Score Semântico">Score: {item.score_cruzamento}</span>
                          )}
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </main>
    </div>
  );
}

function KPICard({ title, value, desc, icon, color, isAlert }: any) {
  return (
    <div className={`bg-white rounded-lg p-5 shadow-sm border-l-4 ${color} flex items-center justify-between`}>
      <div>
        <div className="flex items-center gap-2 mb-1.5">
          <h3 className="text-xs font-semibold uppercase tracking-wider text-slate-500">{title}</h3>
        </div>
        <div className={`text-2xl font-bold ${isAlert ? 'text-rose-600' : 'text-slate-800'}`}>
          {value}
        </div>
        <p className="text-xs text-slate-400 mt-1">{desc}</p>
      </div>
      <div className="p-2 bg-slate-50 rounded-full">
        {icon}
      </div>
    </div>
  );
}

function TabButton({ active, onClick, label, count }: any) {
  return (
    <button 
      onClick={onClick}
      className={`px-5 py-2.5 text-sm font-medium rounded-t-md transition-colors border-b-2 ${
        active 
          ? 'bg-white text-primary-700 border-primary-600 border-x border-t border-x-slate-200 border-t-slate-200' 
          : 'bg-transparent text-slate-500 border-transparent hover:text-slate-700 hover:bg-slate-50'
      } flex items-center gap-2`}
      style={{ marginBottom: '-1px' }}
    >
      {label}
      <span className={`text-xs px-2 py-0.5 rounded-full ${active ? 'bg-primary-100 text-primary-800' : 'bg-slate-100 text-slate-500'}`}>
        {count}
      </span>
    </button>
  );
}

function StatusBadge({ type }: { type: string }) {
  if (type === 'ambos') return <span className="inline-block px-2 py-1 bg-emerald-100 text-emerald-800 text-[10px] font-bold uppercase rounded-md tracking-wider">OK / Cruzado</span>;
  if (type === 'apenas_pncp') return <span className="inline-block px-2 py-1 bg-amber-100 text-amber-800 text-[10px] font-bold uppercase rounded-md tracking-wider">Aguardando</span>;
  if (type === 'apenas_aplic') return <span className="inline-block px-2 py-1 bg-orange-100 text-orange-800 text-[10px] font-bold uppercase rounded-md tracking-wider">Sem Edit. PNCP</span>;
  return null;
}

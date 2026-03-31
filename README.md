# Monitor PNCP × APLIC — Sinop/MT

Protótipo de monitoramento de licitações municipais desenvolvido para o **TCE-MT**.

Cruza automaticamente os dados do **PNCP** (Portal Nacional de Contratações Públicas) com o **APLIC** (sistema Oracle do TCE-MT), identificando o que foi publicado em um sistema e não no outro, e alertando quando o prazo de envio vence.

---

## O problema

Os municípios são obrigados a publicar licitações no PNCP e, em até **3 dias úteis**, registrá-las no APLIC. Hoje esse controle é manual — não há como saber facilmente quais licitações estão em um sistema e faltam no outro.

---

## O que foi construído

```
pncp_pipeline/
  main.py               # Coleta dados do PNCP via API (assíncrono, com retry)
  crossmatch.py         # Cruza PNCP × APLIC em 3 etapas, gera Excel
  firebase_sync.py      # Sincroniza resultados com o Firestore
  backfill_firebase.py  # Popula o Firebase com dados históricos
  pncp_sync_daily.sh    # Script shell para automação diária (launchd)
  config.py             # Configurações (UF, endpoints, diretórios)
  input/                # Arquivos APLIC exportados do Oracle (.csv)
  output/               # Excels gerados (ignorados no git)

dashboard/
  index.html            # Dashboard web conectado ao Firestore (sem servidor)
```

---

## Como funciona

### 1. Coleta PNCP
```bash
python pncp_pipeline/main.py
# Gera: output/pncp_contratacoes_MT_YYYYMMDD.xlsx
```
Busca todas as licitações publicadas no PNCP para o estado de MT na data informada, via API pública (`pncp.gov.br/api`). Filtra municípios + estado, normaliza JSON aninhado e exporta para Excel.

### 2. Crossmatch PNCP × APLIC
```bash
python pncp_pipeline/crossmatch.py \
  --pncp output/pncp_contratacoes_MT_20260101_20260325.xlsx \
  --aplic input/aplic_sinop.csv \
  --start 20260101 --end 20260325
```
Cruza os dois datasets em **3 etapas (tiers)**:

| Tier | Critério | Quando usa |
|---|---|---|
| 1 — Semântico + Valor | Similaridade de texto ≥ 85% + valor com ≤ 10% de diferença | Caso principal — mais confiável |
| 2 — CNPJ + Data | Mesmo órgão (CNPJ) + datas com ≤ 30 dias de diferença | Quando o texto varia muito |
| 3 — Estrutural | Mesmo município + número + ano + modalidade | Fallback de último recurso |

O **score** final (0–100) é calculado como:
- 50% similaridade de texto (objeto PNCP vs objetivo/motivo APLIC)
- 30% proximidade de valor estimado
- 20% proximidade de data

Resultado: Excel com 5 abas — Resultados, APLIC Completo, Duplicatas, Resumo e Grid.

### 3. Firebase — sincronização
```bash
# Inserir licitações do dia no Firebase
python pncp_pipeline/firebase_sync.py --date 20260325

# Após rodar o crossmatch, atualizar statusAPLIC
python pncp_pipeline/firebase_sync.py --sync-aplic output/crossmatch_....xlsx

# Popular histórico completo
python pncp_pipeline/backfill_firebase.py --start 20260101 --end 20260326
```

### 4. Dashboard
Abrir `dashboard/index.html` no browser. Conecta diretamente ao Firestore via SDK web.

---

## Estrutura no Firebase

```
municipios/
  sinop/
    apenas_pncp/    ← publicado no PNCP, aguardando APLIC (prazo: 3 dias úteis)
    apenas_aplic/   ← existe no APLIC, sem publicação no PNCP
    ambos/          ← matched nos dois sistemas
```

Campos de cada documento: `orgao`, `modalidade`, `numero`, `ano`, `objeto`, `valor`, `cnpj`, `dataPNCP`, `prazoAplic`, `statusPNCP`, `statusAPLIC`, `alertaAtivo`, `score_cruzamento`, `estrategia_match`.

---

## Órgãos monitorados (Sinop)

| Órgão | CNPJ |
|---|---|
| Prefeitura Municipal de Sinop | 15.024.003/0001-32 |
| Câmara Municipal de Sinop | 00.814.574/0001-01 |
| Instituto de Previdência de Sinop (IPREV) | 00.571.071/0001-44 |

Para adicionar outro município: incluir os CNPJs em `CNPJS_MUNICIPIO` no `firebase_sync.py` e adicionar a entrada correspondente em `DE_PARA_UG_CNPJ` no `crossmatch.py`.

---

## Instalação

```bash
git clone https://github.com/EricGuerrize/pncp-eric.git
cd pncp-eric
pip install -r requirements.txt
```

Credenciais Firebase (não estão no git):
- `firebase_credentials.json` — service account, necessário para o Python escrever no Firestore
- Obter em: Firebase Console → Configurações → Contas de serviço → Gerar nova chave privada

---

## Automação diária

O script `pncp_sync_daily.sh` está configurado para rodar todo dia às 07:00 via `launchd` (macOS). Está **desativado** — para reativar:

```bash
launchctl load ~/Library/LaunchAgents/com.pncp.sync.daily.plist
```

Para desativar novamente:
```bash
launchctl unload ~/Library/LaunchAgents/com.pncp.sync.daily.plist
```

---

## O que falta para produção

| Item | Situação | Dependência |
|---|---|---|
| Coleta PNCP automática | Pronto (desativado) | Só reativar |
| Crossmatch PNCP × APLIC | Pronto (manual) | — |
| Dashboard web | Pronto | Firebase Hosting para acesso externo |
| Atualização APLIC automática | Pendente | Acesso ao banco Oracle (TI) |
| Alertas por e-mail | Pendente | Cloud Functions ou Apps Script |
| Expansão para outros municípios | Pendente | Mapear CNPJs dos novos órgãos |

---

## Dependências principais

| Pacote | Uso |
|---|---|
| `httpx` + `asyncio` | Coleta assíncrona da API PNCP |
| `pandas` + `openpyxl` | Manipulação de dados e geração de Excel |
| `rapidfuzz` | Similaridade de texto no crossmatch |
| `firebase-admin` | Escrita no Firestore via service account |
| `tenacity` | Retry automático nas requisições à API |

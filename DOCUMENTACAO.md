# Monitor PNCP × APLIC — Documentação do Projeto

## Objetivo

Monitorar e cruzar as licitações publicadas no **PNCP** (Portal Nacional de Contratações Públicas — sistema federal) com as licitações registradas no **APLIC** (sistema municipal do TCE-MT) para municípios de Mato Grosso.

O objetivo é identificar:
- Licitações publicadas no PNCP que **ainda não aparecem no APLIC** (possível atraso no registro)
- Licitações registradas no APLIC que **não foram publicadas no PNCP** (possível omissão)
- **Divergências** de valor, modalidade ou objeto entre os dois sistemas

---

## Municípios monitorados (MVP)

| Município | PNCP | Crossmatch APLIC |
|-----------|------|-----------------|
| Lucas do Rio Verde | ✅ 89 licitações | ✅ 57 matches |
| Sinop | ✅ 98 licitações | ✅ 48 matches |
| Jangada | ✅ 8 licitações | ✅ 1 match |
| Rondolândia | ✅ 1 licitação | ⏳ pendente |
| Acorizal | ⚠️ 0 no PNCP | ⏳ pendente |

> O Firebase contém dados PNCP de **107 municípios de Mato Grosso** — qualquer um pode ser consultado no dashboard.

---

## Arquitetura geral

```
┌─────────────────┐     ┌─────────────────┐
│   API PNCP      │     │  Oracle / APLIC  │
│  (pncp.gov.br)  │     │   (rede TCE-MT)  │
└────────┬────────┘     └────────┬─────────┘
         │                       │
         ▼                       ▼
   main.py (coleta)       aplic_extractor.py
   Excel MT completo       CSVs por cidade
         │                       │
         └──────────┬────────────┘
                    ▼
            crossmatch.py
          (compara os dois)
                    │
                    ▼
           firebase_sync.py
          (grava no Firestore)
                    │
                    ▼
          ┌─────────────────┐
          │    Firebase      │
          │   (Firestore)    │
          └────────┬─────────┘
                   │
                   ▼
          dashboard/index.html
          (Vercel — read-only)
```

---

## Scripts do pipeline

### `pncp_pipeline/main.py`
Coleta todas as licitações do estado de MT da API do PNCP.
- Busca todas as modalidades de forma assíncrona
- Salva em `output/pncp_contratacoes_MT_YYYYMMDD.xlsx`

```bash
python main.py --from 20260101 --to 20260423
```

---

### `pncp_pipeline/aplic_extractor.py`
Extrai licitações do Oracle (APLIC/TCE-MT) para os municípios informados.
- Descobre os UG codes de cada município consultando a tabela `ENTIDADE`
- Roda a SQL completa do APLIC parametrizada por cidade e ano
- Atualiza `input/orgaos.json` com UG codes e CNPJs descobertos
- Salva `input/licitacao_{cidade}_{ano}.csv`

```bash
python aplic_extractor.py --cidades rondolandia jangada "lucas do rio verde" --ano 2026
```

---

### `pncp_pipeline/crossmatch.py`
Motor de cruzamento PNCP × APLIC. Usa uma cascata de 3 estratégias:

| Tier | Estratégia | Critério |
|------|-----------|----------|
| 1 | Semântico + Financeiro | Fuzzy objeto ≥ 85% + delta valor ≤ 10% |
| 2 | CNPJ + Data | CNPJ exato + abertura vs publicação ≤ 30 dias |
| 3 | Estrutural | Município + número + ano + modalidade |

Gera Excel com abas: `Resultados`, `APLIC_Completo`, `APLIC_Duplicatas`, `Resumo`, `Grid`.

---

### `pncp_pipeline/firebase_sync.py`
Sincroniza dados com o Firestore:
- `sincronizar(df_pncp)` → carrega todas as licitações PNCP para `apenas_pncp/`
- `sincronizar_crossmatch(df_resultado, municipio)` → move matches para `ambos/`, adiciona `apenas_aplic/`

---

### `pncp_pipeline/pipeline_multicidades.py`
Orquestrador completo para múltiplos municípios. Executa em sequência:
1. Sync PNCP completo → Firebase (todos os municípios MT)
2. Extração Oracle por cidade (opcional com `--skip-oracle`)
3. Crossmatch por cidade
4. Atualização Firebase com resultados

```bash
python pipeline_multicidades.py \
  --cidades rondolandia acorizal jangada "lucas do rio verde" \
  --ano 2026 \
  --skip-oracle \
  --skip-pncp-sync   # quando Firebase já está atualizado
```

---

## Estrutura do Firebase (Firestore)

```
municipios/
  {municipio_slug}/              ← ex: "sinop", "lucas_do_rio_verde"
    nome: "Sinop"
    slug: "sinop"
    ultimaSync: timestamp
    
    apenas_pncp/                 ← publicado no PNCP, aguardando registro no APLIC
      {numeroControlePNCP}/
        municipio, orgao, modalidade, numero, ano
        objeto, valor, cnpj
        dataPNCP, prazoAplic     ← dataPNCP + 3 dias úteis
        statusPNCP: "S"
        statusAPLIC: "pendente"
        alertaAtivo: false/true  ← true quando prazoAplic venceu
    
    apenas_aplic/                ← registrado no APLIC, sem publicação no PNCP
      {cnpj-numero-ano}/
        municipio, orgao, modalidade, numero, ano
        objeto, valor, cnpj, dataAPLIC
        statusPNCP: "N", statusAPLIC: "S"
    
    ambos/                       ← match confirmado nos dois sistemas
      {numeroControlePNCP}/
        (todos os campos acima)
        statusPNCP: "S", statusAPLIC: "S"
        score_cruzamento         ← score do algoritmo de matching
        estrategia_match         ← "tier1", "tier2" ou "tier3"
```

---

## KPIs do Dashboard

### Card "Em Ambos"
Licitações que foram **encontradas nos dois sistemas** (PNCP e APLIC). São os matches confirmados pelo algoritmo de crossmatch. Indica boa cobertura entre os sistemas.

### Card "Apenas PNCP"
Licitações publicadas no PNCP que **ainda não têm registro correspondente no APLIC**. Cada registro tem um prazo de 3 dias úteis para aparecer no APLIC. Após o prazo, vira um **alerta ativo**.

> ⚠️ Alta contagem aqui pode indicar atraso no registro municipal ou licitações não capturadas pelo APLIC.

### Card "Apenas APLIC"
Licitações que existem no APLIC mas **não foram publicadas no PNCP**. Pode indicar:
- Omissão na publicação federal (irregularidade)
- Licitações de modalidades dispensadas de publicação no PNCP
- Diferença de período entre os dados

### Alertas (badge vermelho)
Subconjunto do "Apenas PNCP" onde o prazo de 3 dias úteis já venceu. São os casos que merecem investigação imediata.

### Score de Cruzamento
Cada match em "Em Ambos" tem um score de 0 a 100 calculado com:
- **50%** — similaridade textual do objeto (fuzzy match)
- **30%** — proximidade de valor (delta percentual)
- **20%** — proximidade de data (delta em dias)

| Score | Status |
|-------|--------|
| ≥ 85 | MATCH_CONFIRMADO |
| 70–84 | MATCH_PARCIAL |
| < 70 | SEM_MATCH |

---

## Dashboard Web

**URL:** https://pncp-eric.vercel.app

**Tecnologia:** HTML + JavaScript + Firebase SDK (read-only)

**Filtros disponíveis:**
- Município (carregado dinamicamente do Firestore)
- Ano
- Órgão (Prefeitura, Câmara, etc.)

**Abas:**
- **Em Ambos** — matches confirmados com dados dos dois sistemas
- **Apenas PNCP** — aguardando registro APLIC (com destaque de alertas)
- **Apenas APLIC** — sem publicação PNCP

---

## Mapeamento de Órgãos (orgaos.json)

Arquivo `pncp_pipeline/input/orgaos.json` mapeia o código UG interno do APLIC ao CNPJ usado no PNCP:

```json
{ "ug": "1112309", "municipio": "sinop", "cnpj": "15024003000132", "nome": "Prefeitura Municipal de Sinop" }
```

Este mapeamento é essencial para o crossmatch: o APLIC usa UG codes internos, o PNCP usa CNPJ.

O script `aplic_extractor.py` preenche automaticamente novos municípios neste arquivo ao rodar.

---

## Fluxo de dados resumido

```
Diariamente (ou sob demanda):

1. main.py           → ~4.700 licitações MT do PNCP → Excel
2. firebase_sync.py  → Excel → Firestore (apenas_pncp para ~107 municípios)

Sob demanda (máquina com Oracle):

3. aplic_extractor.py → Oracle → CSVs por cidade
4. crossmatch.py       → PNCP Excel + CSV APLIC → Excel resultado
5. firebase_sync.py    → Excel resultado → Firestore (ambos + apenas_aplic)
```

---

## Configuração de ambiente

Arquivo `.env` na raiz do projeto:

```
ORACLE_USER=...
ORACLE_PASSWORD=...
ORACLE_DSN=ip:1521/servico
FIREBASE_CREDENTIALS_PATH=/caminho/para/firebase_credentials.json
```

O `firebase_credentials.json` é a chave de conta de serviço do Firebase (não commitada no git).

---

## Limitações conhecidas

- **Acorizal** não tem licitações publicadas no PNCP (zero registros no sistema federal)
- **Rondolândia** tem apenas 1 licitação no PNCP — cidade muito pequena
- O crossmatch depende de dados textuais de qualidade; objetos muito vagos reduzem a precisão
- O pipeline Oracle só roda na rede interna do TCE-MT

# Projeto PNCP × APLIC — TCE-MT

## Visão Geral

Pipeline de engenharia de dados desenvolvido para o **Tribunal de Contas de Mato Grosso (TCE-MT)**, com dois objetivos centrais:

1. **Coletar** automaticamente as contratações públicas do estado de Mato Grosso publicadas no [Portal Nacional de Contratações Públicas (PNCP)](https://pncp.gov.br)
2. **Cruzar** esses dados com os registros lançados pelos jurisdicionados no sistema interno **APLIC** (Oracle), para fins de auditoria e conformidade

---

## Problema a Resolver

Os órgãos públicos municipais e estaduais de MT têm obrigação legal de publicar suas licitações no PNCP **e** de registrá-las no APLIC. O projeto visa detectar automaticamente:

- Licitações que constam no PNCP mas não foram registradas no APLIC
- Licitações com divergências de valor entre os dois sistemas
- Registros inconsistentes (número, modalidade, objeto)

---

## Arquitetura do Sistema

```
Coleta PNCP (API)
    └── collector.py        → requisições async, todas as modalidades, todas as páginas
    └── normalizer.py       → flatten do JSON aninhado
    └── dataset_builder.py  → monta DataFrame, filtra esferas M/E, seleciona colunas
    └── excel_exporter.py   → sanitiza e exporta .xlsx

Cruzamento PNCP × APLIC
    └── crossmatch.py       → módulo completo de cruzamento em 3 tiers
```

---

## O que já está implementado

### Pipeline de Coleta (`main.py`)
- [x] Coleta async de todas as modalidades de licitação para MT
- [x] Retry automático (até 5x por requisição)
- [x] Normalização e limpeza do campo `objetoCompra`
- [x] Filtro de esferas: apenas Municipal (M) e Estadual (E)
- [x] Export para `.xlsx` em `pncp_pipeline/output/`
- [x] Logs em `pncp_pipeline/logs/`

### Módulo de Cruzamento (`crossmatch.py`)
- [x] Preparação e deduplicação de ambos os DataFrames
- [x] **Tier 1** — merge forte: `CNPJ + número + ano + modalidade`
- [x] **Tier 2** — fallback: `município + número + ano + modalidade`
- [x] **Tier 3** — fuzzy match por similaridade de texto (`rapidfuzz`)
- [x] Cálculo de `fuzzy_score` (similaridade entre `objetoCompra` e `Objetivo`)
- [x] Cálculo de `delta_percentual` entre valores estimados
- [x] Classificação: `MATCH_CONFIRMADO` / `MATCH_PARCIAL` / `SEM_MATCH`
- [x] CLI standalone: `python crossmatch.py pncp.xlsx aplic.csv`
- [x] Integração condicional no `main.py` (Step 6)

---

## O que falta fazer

### Curto prazo — Validação

- [ ] **Testar o crossmatch** com o arquivo `licitacao_lrv_2026.csv` (Lucas do Rio Verde)
  - Verificar se licitação `00000000001/2026` (Dispensa, UG 1111319) aparece como `MATCH_CONFIRMADO`
  - Confirmar que registros sem correspondência aparecem como `SEM_MATCH`
  - Validar se os scores fazem sentido na prática
- [ ] Ajustar limiares `LIMIAR_MATCH_CONFIRMADO` (85) e `LIMIAR_MATCH_PARCIAL` (70) se necessário
- [ ] Revisar o `MAPA_MODALIDADE_APLIC_PARA_PNCP` com os códigos reais do APLIC

### Médio prazo — Escalabilidade

- [ ] **Resolver o mapeamento UG → CNPJ para todos os municípios de MT** (~141 municípios)
  - **Opção A (ideal):** Solicitar ao supervisor que modifique a query Oracle para incluir o CNPJ no SELECT e remover o filtro `WHERE P.ENT_CODIGO = '1111319'`
  - **Opção B (alternativa):** Criar query Oracle separada que retorna apenas `(cod_ug, municipio, cnpj)` para MT inteiro e carregar como CSV de referência

- [ ] Testar com export APLIC contendo múltiplos municípios
- [ ] Validar performance com volume real (todos os municípios de MT em um mês)

### Longo prazo — Automação e Entrega

- [ ] Implementar agendamento automático (execução diária via `scheduler.py` ou cron)
- [ ] Adicionar argumento de linha de comando para datas: `python main.py --inicio 20260201 --fim 20260228`
- [ ] Criar relatório de auditoria automatizado com resumo das divergências
- [ ] Avaliar integração direta com banco Oracle do APLIC (sem necessidade de CSV manual)

---

## Como Executar

### Coleta PNCP
```bash
cd pncp_pipeline
python main.py
```
Output: `pncp_pipeline/output/pncp_contratacoes_MT_YYYYMMDD_YYYYMMDD.xlsx`

### Cruzamento standalone
```bash
cd pncp_pipeline
python crossmatch.py output/pncp_contratacoes_MT_*.xlsx input/licitacao_lrv_2026.csv
```
Output: `pncp_pipeline/output/crossmatch_pncp_contratacoes_MT_*.xlsx`

### Pipeline completo (coleta + cruzamento automático)
Coloque o CSV do APLIC em `pncp_pipeline/input/licitacao_lrv_2026.csv` e execute:
```bash
python main.py
```
O Step 6 detecta o arquivo e executa o cruzamento automaticamente.

---

## Dependências

```
pandas
openpyxl
aiohttp
rapidfuzz>=3.0.0
```

Instalar:
```bash
pip install -r requirements.txt
```

---

## Estrutura de Diretórios

```
pncp_pipeline/
├── main.py                  # Orquestrador do pipeline
├── collector.py             # Coleta async da API PNCP
├── normalizer.py            # Normalização do JSON
├── dataset_builder.py       # Construção e limpeza do DataFrame
├── excel_exporter.py        # Export para .xlsx
├── crossmatch.py            # Cruzamento PNCP × APLIC
├── config.py                # Configurações centrais
├── input/                   # Arquivos APLIC (CSV do Oracle)
├── output/                  # Arquivos gerados (.xlsx)
└── logs/                    # Logs de execução
```

---

## Contexto Institucional

| Item | Detalhe |
|------|---------|
| Órgão | TCE-MT (Tribunal de Contas de Mato Grosso) |
| Sistema externo | PNCP (Portal Nacional de Contratações Públicas) |
| Sistema interno | APLIC (Oracle — base `aplic2008`) |
| Escopo geográfico | Estado de Mato Grosso (UF = MT) |
| Caso de teste atual | Município de Lucas do Rio Verde (UG `1111319`) |
| CNPJ mapeado | `24.772.246/0001-40` |

# Resultados do Crossmatch — Cuiabá (Prefeitura) × PNCP (Fevereiro 2026)

## 1. Contexto da Análise

| Item | Detalhe |
|------|---------|
| **Fonte A (PNCP)** | `pncp_contratacoes_MT_20260201_20260228.xlsx` — contratações publicadas em fevereiro/2026 para todo MT |
| **Fonte B (APLIC)** | `licitacao_pref_cuiaba_2026.csv` — extrato Oracle do APLIC/TCE-MT, Prefeitura de Cuiabá (Cód. UG `1113125`) |
| **CNPJ da Prefeitura** | `03.533.064/0001-46` |
| **Período PNCP** | 01/02/2026 a 28/02/2026 |
| **Total PNCP processado** | 1.078 registros (MT inteiro) → 1.051 após deduplicação |
| **Total APLIC Cuiabá** | 5 registros (todos Pregão Eletrônico, modalidade 13) |

---

## 2. Os 5 registros APLIC (o que está no TCE-MT)

Todos os 5 registros são **Pregões Eletrônicos** da Prefeitura de Cuiabá para o exercício de 2026:

| Nº Licitação | Nº Puro | Objeto | Valor Estimado (R$) |
|-------------|---------|--------|--------------------:|
| 00000000001/2026 | 1 | Fornecimento de material de cama e banho — SMSOCIAL | 1.917.880,00 |
| 00000000004/2026 | 4 | Serviços de apoio administrativo com dedicação exclusiva de mão de obra | 5.984.461,20 |
| 00000000005/2026 | 5 | Kits de teste de sensibilidade (estesiômetros) para unidades de saúde | 72.092,50 |
| 00000000006/2026 | 6 | Materiais e insumos para unidades de saúde — arboviroses (Dengue/Chikungunya/Zika) | 603.790,55 |
| 00000000007/2026 | 7 | Agregados para construção civil (rocha britada) para vias urbanas e rurais | 6.099.480,00 |

---

## 3. Os 13 registros PNCP (o que está publicado federalmente)

A Prefeitura de Cuiabá tem 13 registros no PNCP de fevereiro/2026:

| Nº Compra | Nº Puro | Ano | Modalidade | Objeto (resumo) | Valor Estimado (R$) |
|-----------|---------|-----|-----------|-----------------|--------------------:|
| 011/2025/PMC | 11 | 2025 | Concorrência Eletrônica | Recuperação de vias — Morada da Serra, Nova Canaã, Parque Atalaia | 5.039.053,36 |
| 001/2026/PMC | 1 | 2026 | Credenciamento | Clínicas veterinárias — bem-estar animal | 1.302.035,70 |
| 007/2025/PMC | 7 | 2025 | Credenciamento | Entidades de classe para consignações em folha | 0 |
| 020/2025/PMC | 20 | 2025 | Pregão Eletrônico | Fornecimento de energia elétrica ACL por 60 meses | 23.494.778,64 |
| **006/2026/PMC** | **6** | **2026** | **Pregão Eletrônico** | **Materiais para saúde — arboviroses dengue** | **603.790,55** |
| **007/2026/PMC** | **7** | **2026** | **Pregão Eletrônico** | **Agregados para construção civil (rocha britada)** | **6.099.480,00** |
| 009/2026/PMC | 9 | 2026 | Pregão Eletrônico | Testes rápidos de dengue (100.000 unidades) | 579.000,00 |
| 008/2026/PMC | 8 | 2026 | Pregão Eletrônico | Transporte de malote e material biológico | 32.868,00 |
| 001 | 1 | 2026 | Dispensa | Serviços gráficos — capa e papel personalizado | 31.800,00 |
| 01 | 1 | 2026 | Dispensa | Aquisição de viga e pilares treliçados | 0 |
| 001-2026-SMADES-SPDU | 1 | 2026 | Dispensa | Materiais de construção — Horto Florestal | 0 |
| 002-2026-SMADES-SPDU | 2 | 2026 | Dispensa | Galões de água mineral 20L | 0 |
| 0000001 | 1 | 2026 | Dispensa | Cadeira de rodas para PCD | 2.387,12 |

> **Observação:** Cuiabá usa o formato `NNN/AAAA/PMC` nos números de compra — diferente do padrão `00000000NNN/AAAA` do APLIC. O algoritmo extrai o número puro corretamente (`006/2026/PMC` → `6`).

---

## 4. Resultado do Cruzamento

| Status | Registros | Interpretação |
|--------|-----------|---------------|
| `MATCH_CONFIRMADO` | **1** | PE #6 — mesmo objeto, mesmo valor, score 96% |
| `SEM_MATCH` | **1.050** | Sem correspondência no APLIC |
| `APENAS_APLIC` | **0** ⚠️ | Ver observação abaixo |

---

## 5. Análise Detalhada dos 5 registros APLIC

### PE #6 — `MATCH_CONFIRMADO` ✅

| Campo | PNCP | APLIC |
|-------|------|-------|
| Número | `006/2026/PMC` | `00000000006/2026` |
| Modalidade | Pregão Eletrônico | Pregão Eletrônico |
| Objeto | Materiais e insumos para saúde — Dengue/Chikungunya/Zika | Mesma descrição |
| Valor Estimado | R$ 603.790,55 | R$ 603.790,55 |
| Fuzzy Score | **96,1%** | — |
| Validação Financeira | **OK** | — |

Confirmação plena: mesma licitação, dados consistentes nos dois sistemas.

---

### PE #7 — Alerta: match estrutural sem confirmação textual ⚠️

| Campo | PNCP | APLIC |
|-------|------|-------|
| Número | `007/2026/PMC` | `00000000007/2026` |
| Modalidade | Pregão Eletrônico | Pregão Eletrônico |
| Objeto PNCP | "REGISTRO DE PREÇOS PARA O FORNECIMENTO DE AGREGADOS PARA A CONSTRUÇÃO CIVIL..." | — |
| Objeto APLIC | "AQUISICAO DE AGREGADOS PARA A CONSTRUCAO CIVIL SUBSTANCIA MINERAL DE ROCHA BRITADA..." | — |
| Valor Estimado | R$ 6.099.480,00 | R$ 6.099.480,00 |
| Fuzzy Score | **~60%** (abaixo de 70%) | — |
| Status | `SEM_MATCH` | — |

**Conclusão:** É quase certo que é a mesma licitação — mesmo número, mesmo ano, mesma modalidade, mesmo valor exato. O score baixo se deve a uma diferença de prefixo textual: o PNCP usa "REGISTRO DE PREÇOS PARA O FORNECIMENTO DE..." enquanto o APLIC registra apenas "AQUISICAO DE...". Este é um caso de **falso negativo do algoritmo** causado por prefixos padrão que devem ser desconsiderados.

> **Recomendação:** Considerar PE #7 como match confirmado manualmente. A divergência é apenas de formatação do texto, não de conteúdo.

---

### PE #1, #4, #5 — Sem par no PNCP de fevereiro ❓

Estes 3 registros do APLIC não encontraram correspondência confirmada no PNCP de fevereiro/2026:

| Nº | Objeto | Valor (R$) | Possível explicação |
|----|--------|------------|---------------------|
| PE 1 | Material de cama e banho — SMSOCIAL | 1.917.880,00 | Publicado em outro mês no PNCP |
| PE 4 | Serviços de apoio administrativo | 5.984.461,20 | Publicado em outro mês no PNCP |
| PE 5 | Kits estesiômetros para saúde | 72.092,50 | Publicado em outro mês no PNCP |

> **Nota sobre `APENAS_APLIC = 0`:** O algoritmo registrou esses 3 como "encontrados" no Tier 2 por colisão de número — outros órgãos de Cuiabá (Estado, Câmara, etc.) publicaram Pregões com números 1, 4 e 5 em 2026, gerando um falso pareamento por município+número+modalidade. O fuzzy score dessas tentativas ficou abaixo de 70%, resultando em `SEM_MATCH`. Na prática, **esses 3 registros não têm par confirmado no PNCP de fevereiro**.

---

## 6. Registros PNCP sem correspondência no APLIC

Os seguintes registros da Prefeitura de Cuiabá foram publicados no PNCP em fev/2026 mas **não constam no APLIC exportado**:

| Nº Compra | Modalidade | Objeto (resumo) | Valor (R$) |
|-----------|-----------|-----------------|------------|
| 011/2025/PMC | Concorrência Eletrônica | Recuperação de vias — Morada da Serra | 5.039.053,36 |
| 001/2026/PMC | Credenciamento | Clínicas veterinárias | 1.302.035,70 |
| 007/2025/PMC | Credenciamento | Entidades de classe — consignação em folha | 0 |
| 020/2025/PMC | Pregão Eletrônico | Energia elétrica ACL 60 meses | 23.494.778,64 |
| 009/2026/PMC | Pregão Eletrônico | Testes rápidos dengue | 579.000,00 |
| 008/2026/PMC | Pregão Eletrônico | Transporte malote/material biológico | 32.868,00 |
| 001 / 01 / 001-2026-SMADES-SPDU / 0000001 | Dispensa | Vários objetos | — |
| 002-2026-SMADES-SPDU | Dispensa | Galões de água mineral | 0 |

> Esses registros aparecem como `SEM_MATCH` porque o APLIC exportado cobre apenas uma amostra de 5 licitações. Não é possível concluir que são não-conformidades sem um extrato completo do APLIC para Cuiabá.

---

## 7. Conclusões

| Situação | Qtd | Conclusão |
|---------|-----|-----------|
| Confirmado nos dois sistemas | **1** (PE #6) | OK — dados consistentes |
| Match estrutural, falso negativo textual | **1** (PE #7) | Provavelmente OK — revisar manualmente |
| APLIC sem par confirmado no PNCP de fev | **3** (PE #1, #4, #5) | Verificar em outros meses do PNCP |
| PNCP sem correspondência no APLIC exportado | **8** | APLIC exportado é amostra parcial |

### Limitações desta execução

1. **APLIC tem apenas 5 registros de Cuiabá** — amostra muito pequena para conclusões definitivas. Solicitar extrato completo da Prefeitura de Cuiabá.
2. **PNCP coletado apenas em fevereiro** — PE 1, 4 e 5 podem ter sido publicados em janeiro.
3. **Falso negativo no PE #7** — o algoritmo não confirmou um match que é evidente. Isso se deve à diferença de prefixo textual ("REGISTRO DE PREÇOS PARA O FORNECIMENTO DE" vs. "AQUISICAO DE"). Uma melhoria futura seria pré-processar o texto removendo esses prefixos padrão.
4. **Falsos positivos no Tier 2** — em municípios grandes como Cuiabá (vários órgãos), o match por número+município pode gerar colisões entre a Prefeitura e outros órgãos. O CNPJ no SELECT da query Oracle resolveria isso definitivamente.

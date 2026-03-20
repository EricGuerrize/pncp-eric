# Resultados Crossmatch PNCP × APLIC — Sinop/MT (Jan–Mar 2026)

## Parâmetros

| Item | Valor |
|---|---|
| Período PNCP coletado | 01/01/2026 – 20/03/2026 |
| Arquivo PNCP | `pncp_contratacoes_MT_20260101_20260320.xlsx` |
| Arquivo APLIC | `input/aplicSinop.csv` |
| Resultado | `output/crossmatch_sinop_20260101_20260320.xlsx` |
| Total registros PNCP (MT) | 3.155 → 3.035 após deduplicação |
| Total registros APLIC (Sinop) | 54 → 29 após deduplicação |

> **Nota sobre deduplicação APLIC:** os 54 registros originais contêm licitações com múltiplas linhas de lote/item para o mesmo número de licitação. Após deduplicação pela chave (número + ano + modalidade), restaram 29 registros únicos — que são os comparados contra o PNCP.

---

## Entidades mapeadas

| Cód. UG | Nome | CNPJ |
|---|---|---|
| 1118736 | Câmara Municipal de Sinop | 00.814.574/0001-01 |
| 1113257 | Instituto de Previdência de Sinop (IPSINOP) | 00.571.071/0001-44 |
| 1112309 | Prefeitura Municipal de Sinop | 15.024.003/0001-32 |

---

## Totais por status (resultado completo — inclui todo MT)

| Status | Qtde |
|---|---|
| MATCH_CONFIRMADO | 25 |
| MATCH_PARCIAL | 7 |
| APENAS_APLIC | 2 |
| SEM_MATCH (outros municípios MT) | 3.003 |
| **Total** | **3.037** |

> Os 3.003 SEM_MATCH correspondem a contratações publicadas no PNCP para municípios MT sem entrada correspondente no APLIC de Sinop — esperado, pois o APLIC fornecido é exclusivo de Sinop.

---

## Análise por entidade (registros APLIC)

| Entidade | MATCH_CONFIRMADO | MATCH_PARCIAL | APENAS_APLIC | Total matched |
|---|---|---|---|---|
| Prefeitura Municipal de Sinop | 17 | 3 | 2 | 22 |
| Câmara Municipal de Sinop | 6 | 3 | 0 | 9 |
| Instituto de Previdência de Sinop | 2 | 1 | 0 | 3 |
| **Total** | **25** | **7** | **2** | **34** |

De 29 registros APLIC únicos: **27 encontraram par no PNCP** (93%), **2 sem par** (Prefeitura).

---

## Estratégia de match

| Estratégia | Qtde | Descrição |
|---|---|---|
| primario (Tier 1) | 12 | CNPJ + número + ano + modalidade |
| secundario_municipio (Tier 2) | 22 | Município + número + ano + modalidade |
| terciario_fuzzy (Tier 3) | 20 | Fuzzy score de objeto/objetivo |
| sem_par_pncp | 2 | APLIC sem publicação no PNCP |

> Tier 1 (CNPJ-based) foi ativado para os 3 CNPJs de Sinop. Os matches via Tier 2 e Tier 3 ocorrem porque parte das licitações da Prefeitura não encontrou correspondência exata de número/modalidade via CNPJ (provavelmente divergências de numeração entre os sistemas).

---

## Tabela de matches (MATCH_CONFIRMADO e MATCH_PARCIAL)

| Status | Entidade | Nº Licitação (PNCP) | Modalidade | Fuzzy Score | Δ Financeiro | Validação |
|---|---|---|---|---|---|---|
| MATCH_CONFIRMADO | Câmara Municipal | 3 | Dispensa | 100.0 | 0% | OK |
| MATCH_CONFIRMADO | Câmara Municipal | 4 | Dispensa | 100.0 | 0% | OK |
| MATCH_CONFIRMADO | Câmara Municipal | 13 | Dispensa | 89.1 | 0% | OK |
| MATCH_CONFIRMADO | Câmara Municipal | 15 | Dispensa | 88.3 | 0% | OK |
| MATCH_CONFIRMADO | Câmara Municipal | 16 | Dispensa | 88.3 | 0% | OK |
| MATCH_CONFIRMADO | Câmara Municipal | 17 | Inexigibilidade | 100.0 | 0% | OK |
| MATCH_PARCIAL | Câmara Municipal | 5 | Dispensa | 81.6 | 0% | OK |
| MATCH_PARCIAL | Câmara Municipal | 6 | Dispensa | 83.8 | 0% | OK |
| MATCH_PARCIAL | Câmara Municipal | 7 | Dispensa | 83.8 | 0% | OK |
| MATCH_CONFIRMADO | IPSINOP | 2 | Inexigibilidade | 100.0 | 0% | OK |
| MATCH_CONFIRMADO | IPSINOP | 6 | Dispensa | 100.0 | 0% | OK |
| MATCH_PARCIAL | IPSINOP | 2 | Dispensa | 72.3 | 0% | OK |
| MATCH_CONFIRMADO | Prefeitura | 1 | Pregão Eletrônico | 100.0 | 0% | OK |
| MATCH_CONFIRMADO | Prefeitura | 2 | Pregão Eletrônico | 87.4 | 0% | OK |
| MATCH_CONFIRMADO | Prefeitura | 6 | Pregão Eletrônico | 91.1 | 0% | OK |
| MATCH_CONFIRMADO | Prefeitura | 6 | Concorrência El. | 99.0 | 0% | OK |
| MATCH_CONFIRMADO | Prefeitura | 7 | Pregão Eletrônico | 89.2 | 0% | OK |
| MATCH_CONFIRMADO | Prefeitura | 9 | Pregão Eletrônico | 86.9 | 0% | OK |
| MATCH_CONFIRMADO | Prefeitura | 13 | Pregão Eletrônico | 90.8 | 0% | OK |
| MATCH_CONFIRMADO | Prefeitura | 13 | Inexigibilidade | 100.0 | 0% | OK |
| MATCH_CONFIRMADO | Prefeitura | 14 | Pregão Eletrônico | 100.0 | 0% | OK |
| MATCH_CONFIRMADO | Prefeitura | 16 | Pregão Eletrônico | 100.0 | 0% | OK |
| MATCH_CONFIRMADO | Prefeitura | 19 | Inexigibilidade | 97.5 | 0% | OK |
| MATCH_CONFIRMADO | Prefeitura | 20 | Dispensa | 99.2 | 0% | OK |
| MATCH_CONFIRMADO | Prefeitura | 21 | Inexigibilidade | 99.5 | 0% | OK |
| MATCH_CONFIRMADO | Prefeitura | 22 | Pregão Eletrônico | 98.0 | 0% | OK |
| MATCH_CONFIRMADO | Prefeitura | 24 | Inexigibilidade | 100.0 | 0% | OK |
| MATCH_CONFIRMADO | Prefeitura | 25 | Pregão Eletrônico | 100.0 | 0% | OK |
| MATCH_CONFIRMADO | Prefeitura | 26 | Dispensa | 100.0 | 0% | OK |
| MATCH_PARCIAL | Prefeitura | 1 | Concorrência El. | 79.3 | 0% | OK |
| MATCH_PARCIAL | Prefeitura | 8 | Pregão Eletrônico | 82.1 | 0% | OK |
| MATCH_PARCIAL | Prefeitura | 11 | Pregão Eletrônico | 84.0 | 0% | OK |

---

## Registros APENAS_APLIC (sem publicação no PNCP)

Ambos os registros são da **Prefeitura Municipal de Sinop**:

| Nº Licitação | Modalidade | Objetivo |
|---|---|---|
| 00000000002/2026 | Dispensa de Licitação (Eletrônica) | Fornecimento de etiqueta patrimonial para o Depto. de Administração e Serviços |
| 00000000003/2026 | Inexigibilidade de Licitação | Locação de imóvel para estacionamento da Sec. de Planejamento Urbano e Habitação |

> Esses 2 registros constam no APLIC mas não foram encontrados no PNCP para o período consultado. Recomenda-se verificar se foram publicados fora do período 01/01–20/03/2026 ou se há pendência de publicação obrigatória no PNCP.

---

## Validação financeira

Todos os 32 registros com match (CONFIRMADO + PARCIAL) apresentaram **δ financeiro = 0%** e **validação = OK**, indicando que os valores estimados estão idênticos entre PNCP e APLIC.

---

## Observações finais

1. **Taxa de cobertura APLIC:** 27/29 registros únicos (93%) encontraram par no PNCP.
2. **Tier 1 (CNPJ) confirmado:** os 3 CNPJs de Sinop foram corretamente mapeados e ativaram o merge primário.
3. **Matches parciais** (7) têm fuzzy score entre 70–84, indicando pequenas divergências na descrição do objeto entre os dois sistemas — valores financeiros estão corretos.
4. **2 APENAS_APLIC** da Prefeitura merecem atenção para verificação de publicação no PNCP.

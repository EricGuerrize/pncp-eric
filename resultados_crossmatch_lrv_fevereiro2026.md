# Resultados do Crossmatch — Lucas do Rio Verde × PNCP (Fevereiro 2026)

## 1. Contexto da Análise

| Item | Detalhe |
|------|---------|
| **Fonte A (PNCP)** | `pncp_contratacoes_MT_20260201_20260228.xlsx` — contratações publicadas em fevereiro/2026 para todo o estado de MT |
| **Fonte B (APLIC)** | `licitacao_lrv_2026.csv` — extrato Oracle do sistema APLIC/TCE-MT, filtrado por `WHERE P.ENT_CODIGO = '1111319'` (Prefeitura de Lucas do Rio Verde) |
| **Período PNCP** | 01/02/2026 a 28/02/2026 |
| **Período APLIC** | Todo o exercício de 2026 (janeiro em diante) |
| **Total PNCP processado** | 1.078 registros (MT inteiro) → 1.051 após deduplicação |
| **Total APLIC carregado** | 29 registros |

---

## 2. Como o cruzamento funciona

O objetivo do crossmatch é responder três perguntas de auditoria:

| Pergunta | Status no output |
|----------|-----------------|
| A licitação está nos **dois sistemas** com dados consistentes? | `MATCH_CONFIRMADO` ou `MATCH_PARCIAL` |
| A licitação está **só no PNCP** — foi publicada federalmente mas não registrada no TCE-MT? | `SEM_MATCH` |
| A licitação está **só no APLIC** — foi registrada no TCE-MT mas não publicada no PNCP? | `APENAS_APLIC` |

O algoritmo realiza um **cruzamento bidirecional**:
- **Lado PNCP → APLIC** (3 tiers em cascata): tenta parear cada licitação do PNCP com um registro no APLIC usando chaves progressivamente mais flexíveis
- **Lado APLIC → PNCP**: ao final, identifica os registros do APLIC que não foram pareados por nenhum tier e os marca como `APENAS_APLIC`

### Tiers de match (PNCP → APLIC)

| Tier | Chave utilizada | Quando é acionado |
|------|----------------|-------------------|
| **Tier 1 — Primário** | `CNPJ + número + ano + modalidade` | CNPJ do órgão está no dicionário DE-PARA (UG → CNPJ) |
| **Tier 2 — Secundário** | `município + número + ano + modalidade` | CNPJ não está no DE-PARA (fallback por município) |
| **Tier 3 — Fuzzy** | Similaridade de texto do objeto da licitação | Não houve match nos tiers anteriores |

---

## 3. Resultado Geral

| Status | Registros | Interpretação para auditoria |
|--------|-----------|------------------------------|
| `MATCH_CONFIRMADO` | **7** | Licitação presente nos dois sistemas, objeto com ≥ 85% de similaridade — **OK** |
| `MATCH_PARCIAL` | **2** | Presente nos dois sistemas, similaridade entre 70–84% — **requer revisão manual** |
| `SEM_MATCH` | **1.042** | Publicado no PNCP sem correspondência no APLIC |
| `APENAS_APLIC` | **17** | Registrado no APLIC sem publicação correspondente no PNCP |
| **Total** | **1.068** | |

> **Atenção:** Os 1.042 `SEM_MATCH` são quase inteiramente de **outros municípios de MT** — esperado, pois o APLIC exportado cobre só Lucas do Rio Verde. Os 17 `APENAS_APLIC` e os 9 matches são os casos relevantes para auditoria desta cidade.

---

## 4. Detalhamento — Licitações com Match (Lucas do Rio Verde)

Todos os 9 matches da **Prefeitura Municipal** foram encontrados via Tier 1 (chave forte por CNPJ):

| Nº PNCP | Modalidade | Fuzzy Score | Validação Financeira | Status |
|---------|-----------|-------------|----------------------|--------|
| IL 4 | Inexigibilidade | 100,0% | OK | `MATCH_CONFIRMADO` |
| PE 12 | Pregão Eletrônico | 100,0% | SEM_VALOR | `MATCH_CONFIRMADO` |
| DL 2 | Dispensa | 99,0% | OK | `MATCH_CONFIRMADO` |
| PE 13 | Pregão Eletrônico | 98,6% | SEM_VALOR | `MATCH_CONFIRMADO` |
| PE 10 | Pregão Eletrônico | 93,4% | SEM_VALOR | `MATCH_CONFIRMADO` |
| PE 9 | Pregão Eletrônico | 92,6% | OK | `MATCH_CONFIRMADO` |
| PE 8 | Pregão Eletrônico | 89,3% | SEM_VALOR | `MATCH_CONFIRMADO` |
| PE 11 | Pregão Eletrônico | 77,3% | SEM_VALOR | `MATCH_PARCIAL` |
| IL 5 | Inexigibilidade | 75,3% | OK | `MATCH_PARCIAL` |

**Sobre `SEM_VALOR`:** O campo `valorTotalEstimado` está zerado no PNCP para esses Pregões. Não é erro do cruzamento — o órgão não preencheu o valor na publicação federal. É um ponto de atenção para auditoria.

**Sobre `MATCH_PARCIAL` (IL 5 e PE 11):** Os objetos das licitações batem parcialmente, mas há diferenças de redação suficientes para não ultrapassar 85%. Recomenda-se conferência manual para confirmar se são de fato a mesma licitação.

---

## 5. Detalhamento — `APENAS_APLIC` (registrado no TCE-MT sem par no PNCP)

Estes 17 registros foram encontrados no APLIC mas **não têm publicação correspondente no PNCP de fevereiro/2026**. São os principais candidatos à investigação de auditoria:

| Nº Licitação | Modalidade | Valor Estimado (R$) | Objeto (resumo) |
|-------------|-----------|--------------------:|-----------------|
| 00000000002/2026 | Credenciamento | 114.570.163,10 | Serviços especializados em consultas, exames, procedimentos médicos e odontológicos |
| 00000000003/2026 | Pregão Eletrônico | 17.397.775,79 | Manutenções na infraestrutura das vias públicas, setores 08-11 |
| 00000000002/2026 | Concorrência Presencial | 12.363.590,51 | Construção da nova unidade do 49º CIRETRAN |
| 00000000002/2026 | Pregão Eletrônico | 6.318.277,20 | Serviços de tecnologia e segurança da informação |
| 00000000003/2026 | Credenciamento | 6.121.363,20 | Serviços médicos para UPA e CAPS |
| 00000000004/2026 | Pregão Eletrônico | 5.584.758,61 | Fornecimento de tintas e materiais de pintura |
| 00000000014/2026 | Pregão Eletrônico | 4.682.273,29 | Equipamentos para Academia da Terceira Idade e Playground |
| 00000000006/2026 | Pregão Eletrônico | 3.905.510,00 | Medalhas e troféus para eventos municipais |
| 00000000003/2026 | Concorrência Presencial | 2.718.056,41 | Terraplanagem na Estrada Vicinal Linha 13 |
| 00000000015/2026 | Pregão Eletrônico | 2.006.845,00 | Fornecimento e instalação de grama sintética |
| 00000000016/2026 | Pregão Eletrônico | 1.795.660,10 | Camisetas e uniformes esportivos |
| 00000000001/2026 | Concorrência Presencial | 1.665.055,56 | Reforma e ampliação da UBS Bandeirantes |
| 00000000001/2026 | Credenciamento | 631.787,60 | Exames complementares de medicina do trabalho |
| 00000000005/2026 | Pregão Eletrônico | 572.348,32 | Perfuração de 16 poços semiartesianos |
| 00000000007/2026 | Pregão Eletrônico | 516.880,94 | Locação de tendas para eventos municipais |
| 00000000008/2026 | Inexigibilidade | 200.000,00 | Show musical Henrique & Diego — Festa do Milho 2026 |
| 00000000002/2026 | Inexigibilidade | 126.302,54 | Organização de evento técnico-esportivo |

### Por que estão como `APENAS_APLIC`?

Há três possíveis explicações para cada caso:

1. **Publicação anterior a fevereiro:** A licitação foi aberta em janeiro e a publicação no PNCP ocorreu antes do período coletado. Para confirmar, seria necessário coletar o PNCP de janeiro/2026.
2. **Ausência real no PNCP:** O órgão registrou no APLIC mas não cumpriu a obrigação de publicar no portal federal — caso claro de não conformidade.
3. **Número diferente entre sistemas:** A licitação existe no PNCP com numeração divergente, e os tiers de match não conseguiram parear.

---

## 6. Detalhamento — `SEM_MATCH` de Lucas do Rio Verde

Dos 1.042 `SEM_MATCH`, a maioria é de outros municípios de MT. Dentro de Lucas do Rio Verde, os `SEM_MATCH` são da **Câmara Municipal** e do **SAAE** — órgãos cujo Cód. UG não está no extrato APLIC exportado.

| Órgão | CNPJ | Registros | Motivo do SEM_MATCH |
|-------|------|-----------|---------------------|
| Câmara Municipal | 24.772.220/0001-00 | 12 | Cód. UG da Câmara ausente no filtro da query Oracle |
| SAAE | 01.377.043/0001-53 | 2 | Cód. UG do SAAE ausente no filtro da query Oracle |

---

## 7. Conclusões e Próximos Passos

### O que funcionou
- O cruzamento identificou **100% dos registros da Prefeitura** publicados no PNCP em fevereiro/2026 que tinham correspondência no APLIC.
- Os scores de similaridade são altos (maioria acima de 89%), confirmando consistência nos objetos entre os dois sistemas.
- Os 17 registros `APENAS_APLIC` foram corretamente isolados e representam os casos prioritários para auditoria.

### Limitações desta execução e próximos passos

| Limitação | Causa | Solução |
|-----------|-------|---------|
| 17 `APENAS_APLIC` não confirmados | PNCP coletado só em fevereiro; licitações podem ter sido publicadas em janeiro | Coletar PNCP de janeiro/2026 e recruzar |
| Câmara e SAAE sem dados no APLIC | Filtro `WHERE P.ENT_CODIGO = '1111319'` na query Oracle | Solicitar ao supervisor a remoção/expansão do filtro |
| Apenas Lucas do Rio Verde no APLIC | Mesma razão acima | Ampliar o extrato APLIC para todo o MT |
| `SEM_VALOR` em 5 Pregões da Prefeitura | `valorTotalEstimado = 0` no PNCP | Ponto de atenção: órgão não preencheu valor na publicação federal |

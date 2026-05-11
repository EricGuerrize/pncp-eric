# PROMPT PARA CLAUDE - Briefing Projeto APLIC vs PNCP

Copie e cole este prompt quando precisar trabalhar com o Claude:

---

## 📋 BRIEFING DO PROJETO

**Objetivo Principal:**
Fazer cross-reference entre dados de licitações do sistema APLIC (municipal) e do PNCP (Portal Nacional de Contratações Públicas) para o município de Sinop, Mato Grosso, exercício 2026.

**O que é o Cross Matching:**
Comparar dois bancos de dados de licitações (APLIC local vs PNCP federal) e:
- Identificar quais registros APLIC têm correspondência em PNCP (matches)
- Identificar quais registros APLIC não estão em PNCP (gaps)
- Identificar quais registros PNCP não estão em APLIC (possíveis duplicatas/etapas posteriores)
- Gerar relatório visual em Excel mostrando tudo isso

**Por que fazer:**
- Validar consistência entre base local (APLIC) e federal (PNCP)
- Identificar registros que não foram publicados em PNCP
- Investigar duplicatas ou etapas posteriores de licitações
- Documentar gaps para ações corretivas

---

## 🛠️ COMO FUNCIONA (Processo)

**Etapa 1: Dados de Entrada**
```
- aplic_extract.csv: 54 registros do APLIC Sinop 2026
- pncp_final.xlsx: 84 registros do PNCP (extraído do Portal)
```

**Etapa 2: Scripts**
```
1. pncp_extractor.py
   └─ Lê pncp_final.xlsx
   └─ Converte para pncp_extract.csv (formato padrão)
   └─ Extrai CNPJ do hyperlink

2. cross_matching.py
   └─ Lê aplic_extract.csv
   └─ Lê pncp_extract.csv
   └─ Compara registros por:
      • UG (Unidade Gestora deve bater)
      • DATA (±5 dias = 30 pts, ±15 dias = 15 pts)
      • VALOR (±10% = 20 pts)
      • MODALIDADE (bônus 10 pts)
   └─ Gera cross_resultado.xlsx (6 abas)
```

**Etapa 3: Saída**
```
cross_resultado.xlsx com:
├─ Aba 1: APLIC_Completo (54 registros + coluna Match_PNCP)
├─ Aba 2: PNCP_Completo (84 registros + coluna Match_APLIC)
├─ Aba 3: MATCHES (registros linkados com sucesso)
├─ Aba 4: Apenas_APLIC (não encontrados em PNCP)
├─ Aba 5: Apenas_PNCP (não encontrados em APLIC)
└─ Aba 6: Resumo (estatísticas consolidadas)
```

---

## 🎯 OBJETIVOS ESPECÍFICOS

### Objetivo 1: Validar Coverage
- De 54 APLIC, quantos foram encontrados em PNCP?
- Taxa de cobertura esperada: 80%+ é aceitável

### Objetivo 2: Identificar Gaps APLIC
- Quais registros APLIC não têm par em PNCP?
- Investigar:
  - Foram publicados com outro nome/modalidade?
  - Não foram publicados em PNCP?
  - Dados estão incompletos?

### Objetivo 3: Identificar Gaps PNCP
- Quais registros PNCP não têm par em APLIC?
- Investigar:
  - São duplicatas (mesma licitação em múltiplas fases)?
  - São etapas posteriores (julgamento, ata)?
  - São registros legítimos que faltam em APLIC?

### Objetivo 4: Documentação
- Gerar relatório técnico explicando:
  - Qual foi a metodologia de matching
  - Quais foram os achados principais
  - Recomendações para ações corretivas

---

## 📊 O QUE QUEREMOS DO EXCEL

**Cada aba deve responder uma pergunta:**

| Aba | Pergunta | Ação |
|-----|----------|------|
| APLIC_Completo | Quais constam no APLIC? | Validar dados |
| PNCP_Completo | Quais constam no PNCP? | Referência |
| MATCHES | O que foi linkado? | Spot check (validar 10 aleatórios) |
| Apenas_APLIC | Por quê não estão em PNCP? | Investigar causa raiz |
| Apenas_PNCP | São duplicatas ou legítimos? | Categorizar cada um |
| Resumo | Qual é a visão geral? | Extrair insights |

---

## 🔧 PRÓXIMAS AÇÕES ESPERADAS

Depois do Excel gerado:

1. **Análise de Gaps**
   - Revisar "Apenas_APLIC": Por quê 8 não estão em PNCP?
   - Revisar "Apenas_PNCP": 35 são duplicatas ou legítimos?

2. **Validação de Matches**
   - Spot check: 10 matches aleatórios fazem sentido?
   - Se 80%+ estão OK → matching está bom

3. **Relatório Final**
   - Resumo executivo: X APLIC, Y PNCP, Z matches, A gaps APLIC, B gaps PNCP
   - Recomendações: O que fazer com os gaps?

4. **Próximas Iterações**
   - Se tiver novos dados APLIC → rodar scripts novamente
   - Excel será atualizado automaticamente

---

## 💡 CONTEXTO TÉCNICO

**Estrutura de Dados:**

APLIC (54 registros):
```
ID | UG | DATA | NUMERO | MODALIDADE | VALOR | OBJETIVO | RESPONSAVEL | SITUACAO
```

PNCP (84 registros):
```
ID | Data | CNPJ | Órgão | Unidade | Modalidade | Valor | Link
```

**Matching Strategy:**
- UG OBRIGATÓRIO (deve conter "SINOP")
- DATA: ±5 dias = match (maior peso)
- VALOR: ±10% = match (médio peso)
- MODALIDADE: similar = bônus

**Score Final:**
- 80-100: Match confiável
- 60-80: Match questionável (revisar)
- <60: Não é match

---

## ✅ CHECKLIST DE SUCESSO

- [x] aplic_extract.csv: 54 registros ✓
- [ ] pncp_extractor.py: rodado com sucesso
- [ ] pncp_extract.csv: 84 registros gerado
- [ ] cross_matching.py: rodado com sucesso
- [ ] cross_resultado.xlsx: gerado com 6 abas
- [ ] Análise de gaps completada
- [ ] Relatório final entregue

---

## 🚀 COMO USAR ESTE PROMPT

**Se precisar rodar os scripts:**
```
"Rodar os scripts no projeto para gerar cross_resultado.xlsx"
```

**Se precisar analisar os dados:**
```
"Analisar a aba MATCHES do cross_resultado.xlsx e fazer spot check de 10 registros aleatórios"
```

**Se precisar gerar relatório:**
```
"Gerar relatório técnico sobre gaps encontrados no cross matching APLIC vs PNCP"
```

---

**FIM DO BRIEFING**

Use este prompt como referência para conversas futuras no projeto.

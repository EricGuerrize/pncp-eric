# Como Prosseguir — PNCP × APLIC (TCE-MT)

## Estado atual do projeto

O pipeline está funcional e testado. O que já existe:

- **Coleta automática** do PNCP para MT (`main.py` + `collector.py`)
- **Cruzamento bidirecional** PNCP × APLIC (`crossmatch.py`) com 3 cenários de auditoria:
  - `MATCH_CONFIRMADO` / `MATCH_PARCIAL` → licitação nos dois sistemas
  - `SEM_MATCH` → publicada no PNCP, sem registro no APLIC
  - `APENAS_APLIC` → registrada no APLIC, sem publicação no PNCP
- **Resultado documentado** do teste com Lucas do Rio Verde (`resultados_crossmatch_lrv_fevereiro2026.md`)

---

## Próximos passos — em ordem de prioridade

### 1. Validar os 17 `APENAS_APLIC` de Lucas do Rio Verde

Os 17 registros encontrados no APLIC sem correspondência no PNCP de fevereiro precisam ser investigados. Antes de concluir que são não-conformidades, é necessário:

- **Coletar o PNCP de janeiro/2026** (as licitações podem ter sido publicadas antes de fevereiro):
  ```bash
  cd pncp_pipeline
  # Altere main.py: asyncio.run(run_pipeline(data_inicial="20260101", data_final="20260131"))
  python main.py
  ```
- Recruzar com o novo arquivo gerado e verificar se os 17 casos aparecem

---

### 2. Expandir o extrato APLIC (solicitar ao supervisor)

O CSV atual tem apenas a Prefeitura de Lucas do Rio Verde (`WHERE P.ENT_CODIGO = '1111319'`). Para um teste mais completo, pedir ao supervisor que ajuste a query Oracle de duas formas:

**Opção A — Todos os órgãos de Lucas do Rio Verde** (Câmara + SAAE + Prefeitura):
```sql
-- Remover ou ampliar o WHERE para incluir todos os ENT_CODIGO de Lucas do Rio Verde
WHERE P.MUNICIPIO = 'LUCAS DO RIO VERDE'
```

**Opção B — Todos os municípios de MT** (escala completa):
```sql
-- Remover o filtro de entidade completamente
-- Adicionar CNPJ no SELECT para eliminar a necessidade do dicionário DE-PARA:
SELECT P.ENT_CODIGO, P.CNPJ_DIREITO_PUBLICO AS CNPJ, P.MUNICIPIO, ...
```

A **Opção B** é a ideal para o projeto completo. Com o CNPJ no SELECT, o dicionário `DE_PARA_UG_CNPJ` no `crossmatch.py` se torna desnecessário — o join passa a ser direto por CNPJ.

---

### 3. Adicionar Cuiabá ao cruzamento

A extração de fevereiro/2026 contém **35 entidades de Cuiabá** (188 registros), incluindo:
- Município de Cuiabá (`03533064000146`)
- Câmara Municipal de Cuiabá (`33710823000160`)
- Estado de Mato Grosso (múltiplos CNPJs)
- Secretarias estaduais, Tribunal de Justiça, TCE-MT, etc.

Para cruzar com Cuiabá, será necessário o extrato APLIC dessas entidades. Solicitar ao supervisor um CSV similar ao de Lucas do Rio Verde, mas para Cuiabá.

Ao receber, adicionar os mapeamentos no `DE_PARA_UG_CNPJ` em `crossmatch.py`:
```python
DE_PARA_UG_CNPJ: dict[tuple[str, str], str] = {
    ("1111319", "lucas do rio verde"): "24772246000140",
    ("XXXXXXX", "cuiaba"): "03533064000146",  # Prefeitura de Cuiabá
    # ... demais entidades
}
```

---

### 4. Automatizar a coleta periódica

O pipeline já tem suporte a agendamento (`scheduler.py`). Para rodar diariamente:

```bash
# Opção simples: cron no Mac
crontab -e
# Adicionar: 0 6 * * * cd /caminho/pncp_pipeline && python main.py
```

Ou usar o `scheduler.py` que já existe no projeto.

---

### 5. Testar instalação no outro computador

Ao retomar em outro computador:

```bash
# 1. Clonar o repositório
git clone https://github.com/EricGuerrize/pncp-eric.git
cd pncp-eric

# 2. Instalar dependências
pip install -r requirements.txt

# 3. Colocar o CSV do APLIC na pasta de input
cp licitacao_lrv_2026.csv pncp_pipeline/input/

# 4. Rodar o crossmatch standalone (sem precisar coletar da API)
cd pncp_pipeline
python crossmatch.py output/pncp_contratacoes_MT_20260201_20260228.xlsx input/licitacao_lrv_2026.csv

# 5. Ou rodar o pipeline completo (coleta + cruzamento automático)
python main.py
```

O arquivo de output será gerado em `pncp_pipeline/output/crossmatch_*.xlsx`.

---

### 6. Melhorias futuras (quando o projeto estiver estável)

- [ ] Incluir CNPJ no SELECT da query Oracle (elimina o `DE_PARA` manual)
- [ ] Relatório de auditoria automatizado em PDF ou HTML
- [ ] Dashboard de conformidade por município
- [ ] Argumento de linha de comando para datas: `python main.py --inicio 20260101 --fim 20260131`

---

## Arquivos importantes

| Arquivo | O que é |
|---------|---------|
| `pncp_pipeline/crossmatch.py` | Módulo central de cruzamento |
| `pncp_pipeline/main.py` | Orquestrador do pipeline completo |
| `pncp_pipeline/config.py` | Configurações (paths, API, APLIC_CSV_PATH) |
| `pncp_pipeline/input/licitacao_lrv_2026.csv` | CSV do APLIC (não commitado — colocar manualmente) |
| `pncp_pipeline/output/` | Resultados gerados (.xlsx) |
| `resultados_crossmatch_lrv_fevereiro2026.md` | Análise detalhada do teste realizado |
| `OBJETIVOS.md` | Visão geral do projeto |
| `problemas_cruzamento_pncp_aplic.md` | Documentação técnica dos problemas e soluções |

> **Atenção:** O arquivo `licitacao_lrv_2026.csv` não está no repositório (dados sensíveis). Ele deve ser copiado manualmente para `pncp_pipeline/input/` antes de rodar o crossmatch.

# Instruções para o Antigravity — Pipeline Multicidades

Execute os passos abaixo **em ordem** no terminal, dentro do diretório `pncp_pipeline/`.

---

## Pré-requisitos

```bash
cd /caminho/para/o/projeto/pncp\ bruno/pncp_pipeline
```

Verifique que o `.env` existe e tem as credenciais Oracle:

```bash
cat .env
# Deve conter:
# ORACLE_USER=...
# ORACLE_PASSWORD=...
# ORACLE_DSN=...
```

---

## Passo 1 — Teste de conexão Oracle (dry-run)

Este passo só descobre os UG codes sem extrair dados. Serve para verificar se a conexão está funcionando.

```bash
python aplic_extractor.py \
  --dry-run \
  --cidades rondolandia acorizal jangada "lucas do rio verde" \
  --ano 2026
```

**Resultado esperado:** Tabela com UG codes e nomes das entidades encontradas.

Se "lucas do rio verde" aparecer com UG code `1111319`, a conexão está OK.

---

## Passo 2 — Extração APLIC do Oracle

Extrai os dados de licitação APLIC para as 4 cidades, salva CSVs em `input/` e atualiza `orgaos.json`:

```bash
python aplic_extractor.py \
  --cidades rondolandia acorizal jangada "lucas do rio verde" \
  --ano 2026
```

**Resultado esperado:**
- `input/licitacao_rondolandia_2026.csv`
- `input/licitacao_acorizal_2026.csv`
- `input/licitacao_jangada_2026.csv`
- `input/licitacao_lucas_do_rio_verde_2026.csv`
- `orgaos.json` atualizado com UG codes e CNPJs das novas cidades

---

## Passo 3 — Coleta PNCP (pular se já tiver Excel atualizado)

Se não tiver um Excel PNCP recente em `output/`, colete agora:

```bash
python main.py --from 20260101 --to 20260423
```

Verifique que gerou um arquivo `output/pncp_contratacoes_MT_20260423.xlsx` (ou data similar).

---

## Passo 4 — Crossmatch + Firebase (pipeline completo)

```bash
python pipeline_multicidades.py \
  --cidades rondolandia acorizal jangada "lucas do rio verde" \
  --ano 2026
```

O script vai automaticamente:
1. Usar os CSVs APLIC gerados no Passo 2
2. Usar o Excel PNCP mais recente em `output/`
3. Rodar crossmatch para cada cidade
4. Sincronizar resultados para o Firebase (Firestore)

**Resultado esperado:** Resumo no terminal com contagem de matches por município.

---

## Resolução de problemas

### "Nenhuma entidade encontrada para: rondolandia"
- O município pode não ter licitações no APLIC ou o nome pode ter variação.
- Tente com acento: `"rondolândia"` ou verifique o nome exato no APLIC.

### "CSV APLIC não encontrado"
- Certifique-se que o Passo 2 foi concluído com sucesso.
- Use `--skip-oracle` se os CSVs já existem.

### Erro de conexão Oracle
- Verifique que está na rede correta (VPN ou rede interna TCE-MT).
- Verifique as credenciais em `.env`.

### Crossmatch vazio para uma cidade
- A cidade pode não ter CNPJs mapeados em `orgaos.json`.
- Verifique se o `aplic_extractor.py` atualizou o arquivo com CNPJs válidos.

---

## Comandos úteis

```bash
# Verificar orgaos.json atualizado
cat input/orgaos.json

# Ver CSVs gerados
ls -la input/licitacao_*_2026.csv

# Ver Excels de crossmatch gerados
ls -la output/crossmatch_*_2026.xlsx

# Rodar só crossmatch+Firebase sem re-extrair Oracle
python pipeline_multicidades.py \
  --cidades rondolandia acorizal jangada "lucas do rio verde" \
  --ano 2026 \
  --skip-oracle

# Rodar só crossmatch sem Firebase (para testar)
python pipeline_multicidades.py \
  --cidades rondolandia \
  --ano 2026 \
  --skip-oracle \
  --skip-firebase
```

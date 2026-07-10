# Instruções para o Antigravity — Pipeline Multicidades

## O que este pipeline faz

1. Extrai as licitações do Oracle (APLIC/TCE-MT) para as cidades selecionadas
2. Puxa os dados PNCP correspondentes **diretamente do Firebase** (já sincronizados)
3. Faz o crossmatch em memória (Oracle × Firebase)
4. Grava os resultados de volta no Firebase (`ambos/` e `apenas_aplic/`)

> **Nenhum arquivo Excel é necessário.** O Firebase já tem as licitações PNCP
> de 107 municípios de MT — o pipeline lê dali direto.

---

## Pré-requisitos

Verifique que o `.env` existe em `pncp_pipeline/` (ou na raiz do projeto) com:

```
ORACLE_USER=...
ORACLE_PASSWORD=...
ORACLE_DSN=ip_do_banco:1521/nome_do_servico
FIREBASE_CREDENTIALS_PATH=/caminho/absoluto/para/firebase_credentials.json
```

---

## Passo 1 — Atualizar repositório

```bash
cd "pncp bruno"
git pull
```

---

## Passo 2 — Executar o pipeline

Um único comando. Extrai Oracle + cruza com PNCP do Firebase + atualiza Firebase:

```bash
cd pncp_pipeline

python pipeline_multicidades.py \
  --cidades rondolandia acorizal jangada "lucas do rio verde" \
  --ano 2026 \
  --skip-pncp-sync
```

**Flags:**
- `--skip-pncp-sync` → não re-sincroniza PNCP (Firebase já está atualizado)
- Sem `--skip-oracle` → extrai do Oracle (comportamento padrão)

**Resultado esperado no terminal:**
```
PIPELINE MULTICIDADES — RESUMO
============================================================
  rondolandia          ambos:  XX  apenas_aplic:  XX
  acorizal             ambos:  XX  apenas_aplic:  XX
  jangada              ambos:  XX  apenas_aplic:  XX
  lucas_do_rio_verde   ambos:  XX  apenas_aplic:  XX
============================================================
```

Após isso, o dashboard mostrará os resultados atualizados.

---

## Se quiser pular a extração Oracle (usar CSVs já gerados)

```bash
python pipeline_multicidades.py \
  --cidades rondolandia acorizal jangada "lucas do rio verde" \
  --ano 2026 \
  --skip-oracle \
  --skip-pncp-sync
```

---

## Resolução de problemas

### Erro de conexão Oracle
```
Verifique que está na rede interna do TCE-MT e que as credenciais no .env estão corretas.
```

Teste rápido de conexão (sem extrair dados):
```bash
python aplic_extractor.py --dry-run --cidades "lucas do rio verde" --ano 2026
```
Se aparecer o UG code `1111319`, a conexão está OK.

### "Nenhum dado PNCP no Firebase"
O Firebase não tem dados para o município. Execute:
```bash
python pipeline_multicidades.py --cidades <cidade> --ano 2026 --pncp-inicio 20260101 --pncp-fim 20260424
```
Isso coleta o PNCP da API e sincroniza com Firebase antes de fazer o crossmatch.

### `firebase_credentials.json` não encontrado
Adicione no `.env`:
```
FIREBASE_CREDENTIALS_PATH=/caminho/completo/para/firebase_credentials.json
```

### Quero ver os CSVs e Excels gerados
```bash
ls -la input/licitacao_*_2026.csv
ls -la output/crossmatch_*_2026.xlsx
```

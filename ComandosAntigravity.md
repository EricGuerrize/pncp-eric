# Comandos para rodar na máquina com acesso ao banco

## Pré-requisitos

1. Ter o arquivo `firebase_credentials.json` na raiz do projeto (`pncp bruno/`)
2. Ter o `.env` com as credenciais Oracle e o caminho do Firebase:

```
ORACLE_USER=...
ORACLE_PASSWORD=...
ORACLE_DSN=ip_do_banco:1521/nome_do_servico
FIREBASE_CREDENTIALS_PATH=/caminho/completo/para/firebase_credentials.json
```

---

## Passo 1 — Atualizar repositório

```bash
cd "pncp bruno"
git pull
```

---

## Passo 2 — Extrair dados APLIC do Oracle

Puxa as licitações do banco Oracle para as 4 cidades e salva como CSV:

```bash
cd pncp_pipeline
python aplic_extractor.py --cidades rondolandia acorizal jangada "lucas do rio verde" --ano 2026
```

**Resultado esperado:**
- `pncp_pipeline/input/licitacao_rondolandia_2026.csv`
- `pncp_pipeline/input/licitacao_acorizal_2026.csv`
- `pncp_pipeline/input/licitacao_jangada_2026.csv`
- `pncp_pipeline/input/licitacao_lucas_do_rio_verde_2026.csv`

---

## Passo 3 — Coletar licitações do PNCP (se não tiver Excel recente)

Puxa todas as licitações de MT do Portal Nacional:

```bash
python main.py --from 20260101 --to 20260423
```

**Resultado esperado:** arquivo `pncp_pipeline/output/pncp_contratacoes_MT_20260423.xlsx`

> Pule este passo se já existir um `.xlsx` recente na pasta `output/`.

---

## Passo 4 — Pipeline completo (PNCP + APLIC → Firebase + Crossmatch)

Este comando faz tudo de uma vez:
1. Joga **todas** as licitações do PNCP de MT no Firebase
2. Faz o crossmatch das 4 cidades com os dados do APLIC
3. Atualiza o Firebase com os resultados

```bash
python pipeline_multicidades.py \
  --cidades rondolandia acorizal jangada "lucas do rio verde" \
  --ano 2026 \
  --skip-oracle
```

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

Após isso o dashboard em https://pncp-eric.vercel.app mostrará todas as licitações.

---

## Resolução de erros comuns

**`firebase_credentials.json` não encontrado:**
Adicione no `.env`:
```
FIREBASE_CREDENTIALS_PATH=/caminho/completo/para/firebase_credentials.json
```

**Erro de conexão Oracle:**
Verifique que está na rede interna do TCE-MT e que as credenciais no `.env` estão corretas.

**`KeyError: ug_code` ou erro no extractor:**
```bash
python aplic_extractor.py --dry-run --cidades "lucas do rio verde" --ano 2026
```
Se aparecer o UG code `1111319`, a conexão Oracle está OK.

**Quero re-extrair o Oracle também (não pular):**
```bash
python pipeline_multicidades.py \
  --cidades rondolandia acorizal jangada "lucas do rio verde" \
  --ano 2026
```
(sem `--skip-oracle`)

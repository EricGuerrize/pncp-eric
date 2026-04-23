# Comandos para rodar na máquina com acesso ao banco

## Estado atual do Firebase (consultado em 23/04/2026)

| Município         | Licitações PNCP | Crossmatch (ambos) |
|-------------------|-----------------|--------------------|
| Lucas do Rio Verde | 32             | **57 matches** ✅  |
| Sinop             | 50              | **48 matches** ✅  |
| Jangada           | 7               | 1 match (incompleto)|
| Rondolândia       | 1               | 0                  |
| Acorizal          | 0               | 0 (sem dados PNCP) |

**O Firebase já tem os dados PNCP de 107 municípios de MT.** Não é necessário re-sincronizar o PNCP.

O que falta: rodar o crossmatch com os dados do Oracle (APLIC) para as 4 cidades.

---

## Pré-requisitos

1. Ter o arquivo `firebase_credentials.json` na raiz do projeto (`pncp bruno/`)
2. `.env` com as credenciais:

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

```bash
cd pncp_pipeline
python aplic_extractor.py --cidades rondolandia acorizal jangada "lucas do rio verde" --ano 2026
```

**Resultado esperado:**
- `input/licitacao_rondolandia_2026.csv`
- `input/licitacao_acorizal_2026.csv`
- `input/licitacao_jangada_2026.csv`
- `input/licitacao_lucas_do_rio_verde_2026.csv`

---

## Passo 3 — Crossmatch + atualizar Firebase

O Firebase já tem o PNCP. Este comando só faz o crossmatch e atualiza os resultados:

```bash
python pipeline_multicidades.py \
  --cidades rondolandia acorizal jangada "lucas do rio verde" \
  --ano 2026 \
  --skip-oracle \
  --skip-pncp-sync
```

- `--skip-oracle` → usa os CSVs do Passo 2 (não re-extrai do banco)
- `--skip-pncp-sync` → não re-sincroniza o PNCP (já está no Firebase)

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

Após isso o dashboard mostrará as licitações cruzadas para as 4 cidades.

---

## Resolução de erros comuns

**`firebase_credentials.json` não encontrado:**
Adicione no `.env`:
```
FIREBASE_CREDENTIALS_PATH=/caminho/completo/para/firebase_credentials.json
```

**Erro de conexão Oracle:**
Verifique que está na rede interna do TCE-MT e que as credenciais no `.env` estão corretas.

**Teste rápido de conexão Oracle (sem extrair dados):**
```bash
python aplic_extractor.py --dry-run --cidades "lucas do rio verde" --ano 2026
```
Se aparecer o UG code `1111319`, a conexão está OK.

**Quero re-extrair do Oracle também:**
```bash
python pipeline_multicidades.py \
  --cidades rondolandia acorizal jangada "lucas do rio verde" \
  --ano 2026 \
  --skip-pncp-sync
```
(sem `--skip-oracle`)

**Quero refazer tudo do zero (incluindo re-sync PNCP):**
```bash
python pipeline_multicidades.py \
  --cidades rondolandia acorizal jangada "lucas do rio verde" \
  --ano 2026
```
(sem nenhum --skip)

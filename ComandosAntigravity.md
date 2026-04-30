# Comandos para rodar na máquina com acesso ao banco

## Como funciona

O pipeline roda **totalmente na máquina da empresa** (Antigravity):

1. Extrai licitações do **Oracle (APLIC/TCE-MT)** para as cidades selecionadas
2. Busca os dados PNCP correspondentes **direto do Firebase** (sem precisar de Excel)
3. Faz o **crossmatch** em memória (APLIC × PNCP)
4. Grava os resultados de volta no **Firebase** (`ambos/` e `apenas_aplic/`)

> O Firebase já tem licitações PNCP de 107 municípios de MT. Não é necessário
> re-sincronizar o PNCP.

---

## Estado atual do Firebase (consultado em 23/04/2026)

| Município         | Licitações PNCP | Crossmatch (ambos) |
|-------------------|-----------------|--------------------|
| Lucas do Rio Verde | 32             | **57 matches** ✅  |
| Sinop             | 50              | **48 matches** ✅  |
| Jangada           | 7               | 1 match (incompleto)|
| Rondolândia       | 1               | 0                  |
| Acorizal          | 0               | 0 (sem dados PNCP) |

---

## Pré-requisitos

1. Ter o arquivo `firebase_credentials.json` acessível na máquina
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

## Passo 2 — Rodar pipeline (Oracle → crossmatch → Firebase)

```bash
cd pncp_pipeline

python pipeline_multicidades.py \
  --cidades rondolandia acorizal jangada "lucas do rio verde" \
  --ano 2026 \
  --skip-pncp-sync
```

- Extrai APLIC do Oracle automaticamente
- Lê PNCP direto do Firebase (sem Excel)
- Faz crossmatch e atualiza Firebase

---

## Resolução de erros comuns

**Erro de conexão Oracle:**
Verifique rede interna TCE-MT e credenciais no `.env`.

**Teste rápido de conexão Oracle (sem extrair dados):**
```bash
python aplic_extractor.py --dry-run --cidades "lucas do rio verde" --ano 2026
```
Se aparecer o UG code `1111319`, a conexão está OK.

**`firebase_credentials.json` não encontrado:**
Adicione no `.env`:
```
FIREBASE_CREDENTIALS_PATH=/caminho/completo/para/firebase_credentials.json
```

**Quero re-extrair tudo do zero (incluindo re-sync PNCP):**
```bash
python pipeline_multicidades.py \
  --cidades rondolandia acorizal jangada "lucas do rio verde" \
  --ano 2026 \
  --pncp-inicio 20260101 \
  --pncp-fim 20260424
```
(sem nenhum --skip — coleta PNCP da API, sincroniza Firebase, extrai Oracle, faz crossmatch)

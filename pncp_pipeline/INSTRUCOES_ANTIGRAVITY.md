# Instruções para o Antigravity — Pipeline Multicidades

Execute os passos abaixo **em ordem** no terminal, dentro do diretório `pncp_pipeline/`.

---

## Pré-requisitos

```bash
cd /caminho/para/o/projeto/pncp\ bruno/pncp_pipeline
```

Verifique que o `.env` existe e tem as credenciais Oracle **e Firebase**:

```bash
cat .env
# Deve conter:
# ORACLE_USER=...
# ORACLE_PASSWORD=...
# ORACLE_DSN=...
# FIREBASE_CREDENTIALS_PATH=/caminho/absoluto/para/firebase_credentials.json
```

Se a linha `FIREBASE_CREDENTIALS_PATH` não existir no `.env`, adicione apontando para o arquivo
`firebase_credentials.json` que você tem localmente (NÃO commite esse arquivo no git):

```bash
echo 'FIREBASE_CREDENTIALS_PATH=/caminho/absoluto/para/firebase_credentials.json' >> ../.env
```

Substitua `/caminho/absoluto/para/firebase_credentials.json` pelo caminho real do arquivo na sua máquina.
Se o arquivo estiver na raiz do projeto, use algo como:
```
FIREBASE_CREDENTIALS_PATH=/Users/seu_usuario/pncp bruno/firebase_credentials.json
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

## Passo 4 — Pipeline completo (PNCP → Firebase → Crossmatch → Firebase)

Este passo faz tudo de uma vez:
1. Lê o Excel PNCP e joga **todas** as licitações de MT no Firebase (`apenas_pncp`)
2. Para cada cidade: extrai APLIC do Oracle, faz crossmatch, atualiza Firebase (`ambos` / `apenas_aplic`)

```bash
python pipeline_multicidades.py \
  --cidades rondolandia acorizal jangada "lucas do rio verde" \
  --ano 2026 \
  --skip-oracle
```

> **Nota:** `--skip-oracle` pula a extração Oracle e usa os CSVs do Passo 2 que já existem.
> Remova `--skip-oracle` se quiser re-extrair do banco.

**Resultado esperado:**
- Firebase populado com licitações PNCP de **todos** os municípios de MT em `apenas_pncp/`
- Matches das 4 cidades movidos para `ambos/`
- Registros só no APLIC em `apenas_aplic/`
- Resumo no terminal com contagem por cidade

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

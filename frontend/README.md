# Frontend PNCP x APLIC

Painel React para acompanhar o cruzamento entre PNCP e APLIC consumindo a API Go em `go_pipeline/cmd/dashboard`.

## Fluxo atual

- O frontend nao le mais dados do Firestore.
- A tela inicia um job assincro na API Go em `/api/live-crossmatch/start`.
- O progresso e consultado por polling em `/api/live-crossmatch/status`.
- O resultado e exibido em tres buckets: `ambos`, `apenas_pncp` e `apenas_aplic`.
- O drawer de auditoria mostra score final, score por criterio e diferencas do pareamento.

## Desenvolvimento

1. Suba a API Go na porta `5000`.
2. No diretorio `frontend`, instale as dependencias com `npm ci`.
3. Rode `npm run dev`.

O `vite.config.ts` ja possui proxy de `/api` para `http://localhost:5000`.

## Variaveis

- `VITE_API_URL`: sobrescreve a base da API. Por padrao usa `/api/live-crossmatch`.

## Observacoes

- O prazo exibido para registros `apenas_pncp` e calculado no frontend em 5 dias uteis a partir da publicacao PNCP.
- O arquivo legado `dashboard/live_dashboard.html` foi preservado. O React e uma trilha nova, sem quebrar o dashboard antigo.

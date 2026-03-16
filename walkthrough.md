# Walkthrough: Coletor de Contratações do PNCP

O pipeline de coleta de dados do Portal Nacional de Contratações Públicas (PNCP) para a UF Mato Grosso (MT) foi implementado e testado com sucesso.

Abaixo estão os detalhes das alterações, o processo de verificação e os resultados.

## Implementação Finalizada

A solução foi construída utilizando arquitetura limpa em módulos sob a pasta `c:\pythonprojects\PNCP\pncp_pipeline`. Foram criados os seguintes arquivos com as respectivas responsabilidades:

1. **[config.py](file:///c:/pythonprojects/PNCP/pncp_pipeline/config.py)**: Variáveis de configuração como endpoints da API, parâmetros assíncronos (timeout e `tamanhoPagina=50`) e limites organizados.
2. **[pncp_api_client.py](file:///c:/pythonprojects/PNCP/pncp_pipeline/pncp_api_client.py)**: Cliente HTTP assíncrono abstraído através de `httpx`, com estratégia de retentativas exponenciais (`tenacity`) para falhas de requisição.
3. **[collector.py](file:///c:/pythonprojects/PNCP/pncp_pipeline/collector.py)**: Coração do acesso assíncrono. Gerencia semáforos para não exceder limites de concorrência com a API, mapeando primeiro o número máximo de páginas de todas as modalidades simultaneamente e, em seguida, disparando solicitações paralelas para extrair todas as páginas com acompanhamento por uma barra de progresso (`tqdm`).
4. **[normalizer.py](file:///c:/pythonprojects/PNCP/pncp_pipeline/normalizer.py)**: Rotina que achatará dicionários aninhados ("flattening") e mesclará informações da requisição como `data_execucao`, `modalidade_codigo_consultada`, e `pagina_origem` no dado processado, mantendo toda informação bruta retornada inalterada.
5. **[dataset_builder.py](file:///c:/pythonprojects/PNCP/pncp_pipeline/dataset_builder.py)**: Encapsula a conversão da estrutura normalizada em um formato tabular flexível utilizando a biblioteca Pandas.
6. **[excel_exporter.py](file:///c:/pythonprojects/PNCP/pncp_pipeline/excel_exporter.py)**: Cuida da gravação física do artefato `pncp_contratacoes_MT_YYYYMMDD.xlsx` na pasta de saída.
7. **[main.py](file:///c:/pythonprojects/PNCP/pncp_pipeline/main.py)** e **[scheduler.py](file:///c:/pythonprojects/PNCP/pncp_pipeline/scheduler.py)**: Arquivos de entrada que determinam a data D-1 pelo relógio do sistema para consultar ontem. 

## Processos de Validação e Teste

### O Que Foi Testado

1.  **Validação da Integração de Rede:** Forçado uma chamada direta para todas as modalidades na API com as datas mais recentes para observar o comportamento do endpoint, no qual foi identificado através do status `400 Bad Request` que o tamanho máximo de página permitido não processava o requisitado inicialmente de `500`. O valor foi reajustado em conformidade para `50`.
2.  **Tratamento Automático de Falhas (Retry):** Confirmado pelo tracking do HTTP status code que páginas ou modalidades que trazem erros instantâneos disparam um backoff da camada do `tenacity` antes de cair num except capturado pelo logger final.
3.  **Processamento Final de Arquivos:** Após a extração das várias modalidades, foi exportada fielmente a planilha em `pncp_pipeline/output`.

### Resultados da Validação

A execução do arquivo main a partir do terminal validou que o pipeline gera corretamente as pastas necessárias (`logs/` e `output/`):
- O log completo obteve êxito sendo alimentado.
- Um arquivo excel validado por tamanho de 40.65KB foi materializado com sucesso `pncp_pipeline/output/pncp_contratacoes_MT_20260311.xlsx`, mantendo todas as colunas achatas na sintaxe pedida (ex: `orgaoEntidade_cnpj`).

## Como Executar Em Produção Mensalmente/Diariamente

No próprio diretório `c:\pythonprojects\PNCP`, ative o ambiente virtual configurado com as bibliotecas essenciais e execute os métodos:

**Para rodar apenas o script estático uma vez para D-1 (ontem):**
```powershell
.\.venv\Scripts\Activate.ps1
cd pncp_pipeline
python main.py
```

**Para execução perene diária programada para a 01:00 am pelo Scheduler:**
```powershell
.\.venv\Scripts\Activate.ps1
cd pncp_pipeline
python scheduler.py
```
O projeto também se encontra completamente modular para ser engatilhado por CRON, Actions ou Airflow chamando a função `main()` de importação livre.

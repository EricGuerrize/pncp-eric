import os
from pathlib import Path

# Base directories
BASE_DIR = Path(__file__).resolve().parent
LOGS_DIR = BASE_DIR / "logs"
OUTPUT_DIR = BASE_DIR / "output"

# Create directories if they don't exist
LOGS_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

# API Configuration
PNCP_BASE_URL = "https://pncp.gov.br/api/consulta/v1"
CONTRATACOES_ENDPOINT = "/contratacoes/publicacao"

# Query Parameters
UF = "MT"
TAMANHO_PAGINA = 50

# Concurrency & Retry Settings
MAX_CONCURRENT_REQUESTS = 10
MAX_RETRIES = 5
REQUEST_TIMEOUT = 30  # seconds

# Mapping of Modalidades
MODALIDADES = {
    1: "Leilão - Eletrônico",
    2: "Diálogo Competitivo",
    3: "Concurso",
    4: "Concorrência - Eletrônica",
    5: "Concorrência - Presencial",
    6: "Pregão - Eletrônico",
    7: "Pregão - Presencial",
    8: "Dispensa de Licitação",
    9: "Inexigibilidade",
    10: "Manifestação de Interesse",
    11: "Pré-qualificação",
    12: "Credenciamento",
    13: "Leilão - Presencial"
}

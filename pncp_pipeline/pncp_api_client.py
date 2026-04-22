import httpx
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

import config

logger = logging.getLogger(__name__)

class PNCPClient:
    """HTTP async client for the PNCP API with retry logic and timeout."""
    
    def __init__(self):
        self.base_url = config.PNCP_BASE_URL
        self.timeout = httpx.Timeout(config.REQUEST_TIMEOUT)
        self.client = httpx.AsyncClient(base_url=self.base_url, timeout=self.timeout)

    @retry(
        stop=stop_after_attempt(config.MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
        reraise=True
    )
    async def get_contratacoes(self, data_inicial: str, data_final: str, codigo_modalidade: int, pagina: int, uf: str = config.UF):
        endpoint = config.CONTRATACOES_ENDPOINT
        params = {
            "dataInicial": data_inicial,
            "dataFinal": data_final,
            "codigoModalidadeContratacao": codigo_modalidade,
            "uf": uf,
            "pagina": pagina,
            "tamanhoPagina": config.TAMANHO_PAGINA
        }
        try:
            response = await self.client.get(endpoint, params=params)
            response.raise_for_status()
            if response.status_code == 204:
                return {}
            return response.json()
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error for modality {codigo_modalidade}, page {pagina}: {e.response.status_code} - {e.response.text}")
            raise
        except httpx.RequestError as e:
            logger.error(f"Request error for modality {codigo_modalidade}, page {pagina}: {e}")
            raise

    async def close(self):
        await self.client.aclose()

import asyncio
import logging
from tqdm.asyncio import tqdm
import config
from pncp_api_client import PNCPClient

logger = logging.getLogger(__name__)

async def fetch_modality_page(client: PNCPClient, semaphore: asyncio.Semaphore, data_inicial: str, data_final: str, modalidade_cod: int, pagina: int, pbar=None):
    async with semaphore:
        try:
            data = await client.get_contratacoes(
                data_inicial=data_inicial,
                data_final=data_final,
                codigo_modalidade=modalidade_cod,
                pagina=pagina
            )
            if pbar:
                pbar.update(1)
            return {"modalidade_cod": modalidade_cod, "pagina": pagina, "response": data}
        except Exception as e:
            logger.error(f"Failed to fetch modality {modalidade_cod} page {pagina}: {e}")
            if pbar:
                pbar.update(1)
            return {"modalidade_cod": modalidade_cod, "pagina": pagina, "response": None, "error": str(e)}

async def collect_all_data(data_inicial: str, data_final: str):
    client = PNCPClient()
    semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_REQUESTS)
    
    all_results = []
    
    # First pass: fetch page 1 for all modalities
    logger.info("Buscando a primeira página de cada modalidade...")
    first_page_tasks = [
        fetch_modality_page(client, semaphore, data_inicial, data_final, mod_cod, 1)
        for mod_cod in config.MODALIDADES.keys()
    ]
    
    first_page_results = await asyncio.gather(*first_page_tasks)
    
    remaining_pages_args = []
    
    for result in first_page_results:
        mod_cod = result["modalidade_cod"]
        response = result["response"]
        
        all_results.append(result)
        
        if response and "totalPaginas" in response:
            total_pages = response["totalPaginas"]
            logger.info(f"Modalidade {mod_cod} ({config.MODALIDADES[mod_cod]}) - Total de páginas: {total_pages}")
            
            for page in range(2, total_pages + 1):
                remaining_pages_args.append((mod_cod, page))
    
    if remaining_pages_args:
        logger.info(f"Buscando as {len(remaining_pages_args)} páginas restantes...")
        pbar = tqdm(total=len(remaining_pages_args), desc="Coletando páginas adicionais")
        
        subsequent_tasks = [
            fetch_modality_page(client, semaphore, data_inicial, data_final, mod_cod, page, pbar)
            for mod_cod, page in remaining_pages_args
        ]
        
        subsequent_results = await asyncio.gather(*subsequent_tasks)
        all_results.extend(subsequent_results)
        
        pbar.close()
        
    await client.close()
    return all_results

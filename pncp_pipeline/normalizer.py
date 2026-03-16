import logging
from datetime import datetime
import config

logger = logging.getLogger(__name__)

def flatten_dict(d: dict, parent_key: str = '', sep: str = '_') -> dict:
    """
    Flatten a nested dictionary.
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            # Transform lists into string representation to preserve data natively in CSV/Excel.
            items.append((new_key, str(v)))
        else:
            items.append((new_key, v))
    return dict(items)

def normalize_results(raw_results: list, exec_time: datetime) -> list:
    """
    Takes the raw responses from collect_all_data and flattens them into a list of dictionaries.
    """
    normalized_data = []
    data_execucao = exec_time.strftime("%Y-%m-%d %H:%M:%S")
    
    logger.info("Normalizando resultados...")
    
    for result in raw_results:
        response = result.get("response")
        if not response or "data" not in response or not response["data"]:
            continue
            
        mod_cod = result["modalidade_cod"]
        mod_nome = config.MODALIDADES.get(mod_cod, "Desconhecida")
        pagina = result["pagina"]
        
        for item in response["data"]:
            flat_item = flatten_dict(item)
            
            # Add administrative columns
            flat_item["data_execucao"] = data_execucao
            flat_item["modalidade_codigo_consultada"] = mod_cod
            flat_item["modalidade_nome_consultada"] = mod_nome
            flat_item["pagina_origem"] = pagina
            
            normalized_data.append(flat_item)
            
    logger.info(f"Total de registros normalizados: {len(normalized_data)}")
    return normalized_data

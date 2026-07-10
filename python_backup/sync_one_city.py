import sys
import os
sys.path.append(os.path.join(os.getcwd(), "pncp_pipeline"))

import pandas as pd
import logging
import database
import firebase_sync
from google.cloud import firestore

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def sync_city(city_name):
    conn = database.get_connection()
    city_upper = city_name.upper()
    
    logger.info(f"Sincronizando APENAS: {city_upper}")
    
    # 1. Carrega Resultados do Crossmatch para a cidade
    df_results = pd.read_sql("SELECT * FROM crossmatch_results WHERE UPPER(municipio) = ?", conn, params=(city_upper,))
    
    # 2. Carrega Dados PNCP e APLIC para a cidade
    df_pncp = pd.read_sql("SELECT * FROM pncp_data WHERE UPPER(municipio) = ?", conn, params=(city_upper,))
    df_aplic = pd.read_sql("SELECT * FROM aplic_data WHERE UPPER(municipio) = ?", conn, params=(city_upper,))
    
    if df_pncp.empty and df_results.empty:
        logger.warning(f"Nenhum dado encontrado para {city_upper} no banco local.")
        return

    # Mapeamento PNCP
    df_pncp = df_pncp.rename(columns={
        "id": "numeroControlePNCP",
        "municipio": "unidadeOrgao_municipioNome",
        "orgao": "unidadeOrgao_nomeUnidade",
        "cnpj": "orgaoEntidade_cnpj",
        "modalidade": "modalidadeNome",
        "numero": "numeroCompra",
        "ano": "anoCompra",
        "objeto": "objetoCompra",
        "valor": "valorTotalEstimado",
        "data_publicacao": "dataPublicacaoPncp"
    })
    
    # Mapeamento APLIC
    df_aplic = df_aplic.rename(columns={
        "id": "_id_db",
        "municipio": "Municpio",
        "orgao": "UG",
        "cnpj": "_cnpj_mapeado",
        "modalidade": "Modalidade",
        "numero": "_numero_puro",
        "ano": "_ano_extraido",
        "objeto": "Objetivo",
        "valor": "_valor_estimado_float",
        "data_abertura": "Data Abertura",
        "ug_code": "Cd. UG"
    })

    # 3. Sincroniza Base Bruta
    logger.info(f"  - Enviando {len(df_pncp)} registros PNCP...")
    firebase_sync.sincronizar(df_pncp)

    # 4. Sincroniza Resultados do Crossmatch
    if not df_results.empty:
        logger.info(f"  - Enviando {len(df_results)} resultados de auditoria...")
        # Join com PNCP
        df_sync = df_results.merge(df_pncp, left_on="id_pncp", right_on="numeroControlePNCP", how="left")
        # Join com APLIC
        df_sync = df_sync.merge(df_aplic, left_on="id_aplic", right_on="_id_db", how="left", suffixes=("", "_aplic"))
        
        firebase_sync.sincronizar_crossmatch(df_sync, municipio=city_name)
    
    logger.info(f"Sincronizao de {city_upper} CONCLUDO.")
    conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python sync_one_city.py <NOME_DA_CIDADE>")
    else:
        sync_city(" ".join(sys.argv[1:]))

import pandas as pd
import logging
import database
import firebase_sync
from firebase_admin import firestore

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def sync_all_from_sql():
    conn = database.get_connection()
    
    logger.info("Iniciando sincronizao massiva SQL -> Firebase (Q1 2025)...")
    
    # 1. Carrega Resultados do Crossmatch
    df_results = pd.read_sql("SELECT * FROM crossmatch_results", conn)
    
    if df_results.empty:
        logger.warning("Nenhum resultado de crossmatch encontrado no banco. Rode o crossmatch_runner primeiro.")
        return

    # 2. Carrega Dados PNCP e APLIC para enriquecer o sync
    df_pncp = pd.read_sql("SELECT * FROM pncp_data", conn)
    df_aplic = pd.read_sql("SELECT * FROM aplic_data", conn)
    
    # Mapeamento para nomes esperados pelo firebase_sync
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

    # 3. Sincroniza dados PNCP Brutos (vai para 'apenas_pncp')
    logger.info("Sincronizando base bruta do PNCP...")
    firebase_sync.sincronizar(df_pncp)

    # 4. Sincroniza Resultados do Crossmatch (Move para 'ambos' ou cria 'apenas_aplic')
    logger.info("Sincronizando resultados do Crossmatch (Auditando)...")
    municipios = df_results['municipio'].unique()
    
    for mun in municipios:
        logger.info(f"Processando auditoria para {mun}...")
        
        # Filtra os resultados deste município
        df_mun_results = df_results[df_results['municipio'] == mun]
        
        # Join com PNCP
        df_sync = df_mun_results.merge(df_pncp, left_on="id_pncp", right_on="numeroControlePNCP", how="left")
        
        # Join com APLIC (usando id_aplic)
        df_sync = df_sync.merge(df_aplic, left_on="id_aplic", right_on="_id_db", how="left", suffixes=("", "_aplic"))
        
        if not df_sync.empty:
            try:
                firebase_sync.sincronizar_crossmatch(df_sync, municipio=mun)
                logger.info(f"Auditoria concluída para {mun}.")
            except Exception as e:
                logger.error(f"Erro na auditoria de {mun}: {e}")

    logger.info("Sincronização massiva concluída.")
    conn.close()

if __name__ == "__main__":
    sync_all_from_sql()

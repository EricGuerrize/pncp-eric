import pandas as pd
import logging
import database
import crossmatch
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def run_crossmatch_all():
    conn = database.get_connection()
    
    logger.info("Carregando dados do banco para o Crossmatch...")
    
    # 1. Carrega PNCP (RAW)
    df_pncp = pd.read_sql("SELECT * FROM pncp_data", conn)
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
    # PNCP precisa do modalidadeId
    from config import MODALIDADES
    INV_MODALIDADES = {v: k for k, v in MODALIDADES.items()}
    df_pncp['modalidadeId'] = df_pncp['modalidadeNome'].map(INV_MODALIDADES)

    # 2. Carrega APLIC (RAW)
    df_aplic = pd.read_sql("SELECT * FROM aplic_data", conn)
    # Re-mapeia para os nomes originais que o preparar_aplic espera
    df_aplic = df_aplic.rename(columns={
        "municipio": "Município",
        "orgao": "UG",
        "cnpj": "CNPJ",
        "modalidade": "Modalidade",
        "modalidade_cod": "Cod. Modalidade",
        "numero": "Nº Licitação",
        "ano": "Exercício",
        "objeto": "Objetivo",
        "valor": "Valor Estimado",
        "data_abertura": "Data Abertura",
        "ug_code": "Cód. UG"
    })

    if df_pncp.empty or df_aplic.empty:
        logger.warning(f"Uma das bases está vazia. PNCP: {len(df_pncp)}, APLIC: {len(df_aplic)}. Abortando crossmatch.")
        return

    logger.info(f"Iniciando cruzamento: {len(df_pncp)} PNCP vs {len(df_aplic)} APLIC")
    
    # Executa o crossmatch (ele mesmo chama preparar_pncp e preparar_aplic)
    results, _ = crossmatch.crossmatch(df_pncp, df_aplic)
    
    # 3. Salva resultados por município
    # O crossmatch.py pode retornar colunas do PNCP ou APLIC. 
    # Precisamos de uma coluna única e NORMALIZADA de município para o particionamento.
    results['_mun_final'] = results['unidadeOrgao_municipioNome'].fillna(results['Município']).str.upper()
    
    for mun in results['_mun_final'].dropna().unique():
        df_mun = results[results['_mun_final'] == mun]
        if not df_mun.empty:
            database.salvar_crossmatch(df_mun, mun)
            
    logger.info("Crossmatch finalizado e resultados salvos no banco.")
    conn.close()

if __name__ == "__main__":
    run_crossmatch_all()

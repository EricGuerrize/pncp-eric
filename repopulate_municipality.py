import sys
import os
# Adiciona o diretório da pipeline ao path para importar módulos locais
sys.path.append(os.path.join(os.getcwd(), "pncp_pipeline"))

import pandas as pd
import logging
import database
import firebase_sync
from firebase_sync import _slug_municipio, _inicializar_firebase
from google.cloud import firestore

# Configuração de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def wipe_municipio(db, mun_slug):
    """Remove todos os documentos das subcoleções do município para garantir repopulação limpa."""
    logger.info(f"Limpando coleções no Firebase para: {mun_slug}")
    collections = ["apenas_pncp", "apenas_aplic", "ambos"]
    for col_name in collections:
        docs = db.collection("municipios").document(mun_slug).collection(col_name).stream()
        deleted = 0
        batch = db.batch()
        for doc in docs:
            batch.delete(doc.reference)
            deleted += 1
            if deleted % 400 == 0:
                batch.commit()
                batch = db.batch()
        batch.commit()
        if deleted > 0:
            logger.info(f"  - {col_name}: {deleted} documentos removidos.")

def repopulate_city(city_name):
    """Filtra dados locais (Jan-Abr 2025) e sincroniza com Firebase após limpeza."""
    city_upper = city_name.upper()
    mun_slug = _slug_municipio(city_name)
    
    conn = database.get_connection()
    db = _inicializar_firebase()
    
    logger.info(f"")
    logger.info(f"============================================================")
    logger.info(f"REPOPULANDO: {city_upper} (Jan-Abr 2025)")
    logger.info(f"============================================================")
    
    # 1. Limpeza prévia
    wipe_municipio(db, mun_slug)
    
    # 2. Queries filtradas por data (Janeiro a Abril de 2025)
    start_date = "2025-01-01"
    end_date = "2025-04-30"
    
    # Carrega PNCP do período
    query_pncp = "SELECT * FROM pncp_data WHERE UPPER(municipio) = ? AND data_publicacao BETWEEN ? AND ?"
    df_pncp_raw = pd.read_sql(query_pncp, conn, params=(city_upper, start_date, end_date))
    
    # Carrega APLIC do período
    query_aplic = "SELECT * FROM aplic_data WHERE UPPER(municipio) = ? AND data_abertura BETWEEN ? AND ?"
    df_aplic_raw = pd.read_sql(query_aplic, conn, params=(city_upper, start_date, end_date))
    
    # Carrega Resultados de Crossmatch (sem filtro de data direto, faremos join)
    query_results = "SELECT * FROM crossmatch_results WHERE UPPER(municipio) = ?"
    df_results = pd.read_sql(query_results, conn, params=(city_upper,))
    
    if df_pncp_raw.empty and df_aplic_raw.empty:
        logger.warning(f"Nenhum dado encontrado para {city_upper} entre {start_date} e {end_date}.")
        conn.close()
        return

    # Mapeamento PNCP (Nomes esperados pelo firebase_sync)
    df_pncp = df_pncp_raw.rename(columns={
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
    
    # Mapeamento APLIC (Nomes esperados pelo firebase_sync)
    df_aplic = df_aplic_raw.rename(columns={
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

    # 3. Sincroniza Base Bruta PNCP (Vai para 'apenas_pncp')
    logger.info(f"Sincronizando {len(df_pncp)} registros PNCP...")
    firebase_sync.sincronizar(df_pncp)

    # 4. Sincroniza Resultados do Crossmatch (Move para 'ambos' ou cria 'apenas_aplic')
    if not df_results.empty:
        # Filtramos o crossmatch para o período usando os IDs já filtrados
        
        # Matches: devem estar em df_pncp (Jan-Abr)
        df_match = df_results[df_results['status_cruzamento'].isin(['MATCH_CONFIRMADO', 'MATCH_PARCIAL'])]
        df_sync_match = df_match.merge(df_pncp, left_on="id_pncp", right_on="numeroControlePNCP", how="inner")
        # Enriquecer com dados do APLIC (mesmo que o APLIC esteja fora do período, o PNCP manda no período do dashboard)
        # Mas o usuário pediu "dados dos municipios de janeiro ate abril", então idealmente ambos no período.
        # Vamos usar o APLIC filtrado também para garantir.
        df_sync_match = df_sync_match.merge(df_aplic, left_on="id_aplic", right_on="_id_db", how="inner", suffixes=("", "_aplic"))
        
        # Apenas Aplic: deve estar em df_aplic (Jan-Abr)
        df_apenas_aplic = df_results[df_results['status_cruzamento'] == 'APENAS_APLIC']
        df_sync_apenas_aplic = df_apenas_aplic.merge(df_aplic, left_on="id_aplic", right_on="_id_db", how="inner")
        
        df_final_sync = pd.concat([df_sync_match, df_sync_apenas_aplic], ignore_index=True)
        
        if not df_final_sync.empty:
            logger.info(f"Sincronizando {len(df_final_sync)} registros de auditoria (Crossmatch)...")
            firebase_sync.sincronizar_crossmatch(df_final_sync, municipio=city_name)
        else:
            logger.info("Nenhum registro de crossmatch no período filtrado.")
    
    logger.info(f"Sincronização de {city_upper} CONCLUÍDA com sucesso.")
    conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python repopulate_municipality.py <NOME_DA_CIDADE>")
    else:
        city = " ".join(sys.argv[1:])
        repopulate_city(city)

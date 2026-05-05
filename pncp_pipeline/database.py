import sqlite3
import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "monitor_pncp.db"

def get_connection():
    """Retorna uma conexão com o banco de dados SQLite."""
    return sqlite3.connect(DB_PATH)

def inicializar_banco():
    """Cria as tabelas necessárias se não existirem."""
    conn = get_connection()
    cursor = conn.cursor()

    logger.info(f"Inicializando banco de dados em: {DB_PATH}")

    # Tabela para dados do PNCP
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pncp_data (
            id TEXT PRIMARY KEY,
            municipio TEXT,
            orgao TEXT,
            cnpj TEXT,
            modalidade TEXT,
            numero TEXT,
            ano TEXT,
            objeto TEXT,
            valor REAL,
            data_publicacao DATE,
            raw_json TEXT,
            criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Tabela APLIC
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS aplic_data (
            id TEXT PRIMARY KEY,
            municipio TEXT,
            orgao TEXT,
            cnpj TEXT,
            modalidade TEXT,
            modalidade_cod TEXT,
            numero TEXT,
            ano TEXT,
            objeto TEXT,
            valor REAL,
            data_abertura TEXT,
            ug_code TEXT,
            criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Tabela para resultados do Crossmatch
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS crossmatch_results (
            id_pncp TEXT,
            id_aplic TEXT,
            municipio TEXT,
            status_cruzamento TEXT,
            score_composto REAL,
            estrategia_match TEXT,
            sincronizado_firebase INTEGER DEFAULT 0,
            data_cruzamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id_pncp, id_aplic)
        )
    """)

    conn.commit()
    conn.close()
    logger.info("Tabelas criadas/verificadas com sucesso.")

def salvar_pncp(df: pd.DataFrame):
    """Salva um DataFrame do PNCP no banco, mapeando colunas."""
    if df.empty:
        return
    
    conn = get_connection()
    df_db = pd.DataFrame()

    # Mapeamento de colunas PNCP -> SQL
    df_db["id"] = df["numeroControlePNCP"].astype(str).str.replace("/", "_")
    df_db["municipio"] = df.get("unidadeOrgao_municipioNome", "")
    df_db["orgao"] = df.get("unidadeOrgao_nomeUnidade", "")
    df_db["cnpj"] = df.get("orgaoEntidade_cnpj", "").astype(str).str.replace(r"\D", "", regex=True)
    df_db["modalidade"] = df.get("modalidadeNome", "")
    df_db["numero"] = df.get("numeroCompra", "")
    df_db["ano"] = df.get("anoCompra", "")
    df_db["objeto"] = df.get("objetoCompra", "")
    
    if "valorTotalHomologado" in df.columns:
        df_db["valor"] = df["valorTotalHomologado"].fillna(df.get("valorTotalEstimado", 0))
    else:
        df_db["valor"] = df.get("valorTotalEstimado", 0)

    df_db["data_publicacao"] = pd.to_datetime(df.get("dataPublicacaoPncp", ""), errors="coerce")
    
    # Filtra duplicados internos no DF
    df_db = df_db.drop_duplicates(subset=["id"])

    # Filtra o que já existe no banco
    existentes = pd.read_sql("SELECT id FROM pncp_data", conn)["id"].tolist()
    df_db = df_db[~df_db["id"].isin(existentes)]

    if not df_db.empty:
        df_db.to_sql("pncp_data", conn, if_exists="append", index=False, method="multi")
        logger.info(f"{len(df_db)} novos registros PNCP salvos no banco.")
    else:
        logger.info("Nenhum registro PNCP novo para salvar.")
    
    conn.close()

def salvar_aplic(df: pd.DataFrame):
    """Salva um DataFrame do APLIC no banco, mapeando colunas."""
    if df.empty:
        return
    
    conn = get_connection()
    df_db = pd.DataFrame()

    # Mapeamento APLIC robusto para ID
    def _get_col(search_terms):
        # 1. Exact matches first
        for col in df.columns:
            for term in search_terms:
                if term.lower() == col.lower():
                    return df[col]
        # 2. Partial matches as fallback
        for col in df.columns:
            for term in search_terms:
                if term.lower() in col.lower():
                    return df[col]
        return pd.Series([""] * len(df))

    ug_code = _get_col(["_ug_puro", "ug_code", "ug"]).fillna("").astype(str)
    numero = _get_col(["_numero_puro", "licit", "numero"]).fillna("").astype(str)
    ano = _get_col(["_ano_extraido", "exer", "ano"]).fillna("").astype(str)
    cnpj = _get_col(["_cnpj_mapeado", "cnpj"]).fillna("").astype(str).str.replace(r"\D", "", regex=True)

    df_db["id"] = ug_code + "-" + numero + "-" + ano
    
    df_db["municipio"] = _get_col(["_municipio_norm", "município", "municipio"]).fillna("")
    df_db["orgao"] = _get_col(["_orgao_nome", "ug", "orgao"]).fillna("")
    df_db["cnpj"] = cnpj
    df_db["modalidade"] = df.get("Modalidade", df.get("modalidade", ""))
    df_db["modalidade_cod"] = df.get("Cod. Modalidade", df.get("modalidade_cod", ""))
    df_db["numero"] = numero
    df_db["ano"] = ano
    df_db["objeto"] = df.get("Objetivo", df.get("objeto", ""))
    df_db["valor"] = pd.to_numeric(df.get("Valor Estimado", df.get("valor", 0)), errors="coerce")
    df_db["data_abertura"] = pd.to_datetime(df.get("Data Abertura", df.get("data_abertura", "")), errors="coerce")
    df_db["ug_code"] = ug_code

    # Filtra duplicados internos
    df_db = df_db.drop_duplicates(subset=["id"])

    # Filtra o que já existe no banco
    try:
        existentes = pd.read_sql("SELECT id FROM aplic_data", conn)["id"].tolist()
        df_db = df_db[~df_db["id"].isin(existentes)]
    except Exception:
        pass # Tabela pode estar vazia ou não existir

    if not df_db.empty:
        df_db.to_sql("aplic_data", conn, if_exists="append", index=False, method="multi")
        logger.info(f"{len(df_db)} novos registros APLIC salvos no banco.")
    else:
        logger.info("Nenhum registro APLIC novo para salvar.")
        
    conn.close()

def salvar_crossmatch(df: pd.DataFrame, municipio: str):
    """Salva o resultado do crossmatch no banco SQL."""
    if df.empty:
        return
    
    conn = get_connection()
    df_db = pd.DataFrame()

    # Mapeamento do resultado do Crossmatch
    if "numeroControlePNCP" in df.columns:
        df_db["id_pncp"] = df["numeroControlePNCP"].astype(str).str.replace("/", "_")
    else:
        df_db["id_pncp"] = pd.Series(["nan"] * len(df))
    
    # id_aplic: reconstruir o ID de forma robusta
    def _get_col_safe(search_terms, exact_terms=None):
        # 1. Tenta termos exatos primeiro
        if exact_terms:
            for col in df.columns:
                if col in exact_terms:
                    return df[col]
        # 2. Tenta busca por substring
        for col in df.columns:
            for term in search_terms:
                if term.lower() in col.lower():
                    return df[col]
        return pd.Series([""] * len(df))

    ug = _get_col_safe(["ug_code", "ug"], exact_terms=["Cód. UG", "ug_code"])
    numero = _get_col_safe(["numero", "licit", "nº"], exact_terms=["_numero_puro", "Nº Licitação", "numero"])
    ano = _get_col_safe(["ano", "exer"], exact_terms=["_ano_extraido", "Exercício", "ano"])
    
    # Usamos UG + Numero + Ano para garantir unicidade, especialmente no Estado
    df_db["id_aplic"] = ug.fillna("").astype(str) + "-" + numero.fillna("").astype(str) + "-" + ano.fillna("").astype(str)
    
    logger.info(f"IDs Gerados: {df_db['id_aplic'].unique().size} únicos de {len(df_db)}")

    df_db["municipio"] = municipio
    df_db["status_cruzamento"] = df.get("status_cruzamento", "SEM_MATCH")
    df_db["score_composto"] = pd.to_numeric(df.get("score_composto", 0), errors="coerce")
    df_db["estrategia_match"] = df.get("estrategia_match", "")
    df_db["sincronizado_firebase"] = 0

    # Filtra duplicados
    df_db = df_db.drop_duplicates(subset=["id_pncp", "id_aplic"])

    # UPSERT via DELETE + INSERT (simplificado)
    ids_pncp = df_db["id_pncp"].tolist()
    cursor = conn.cursor()
    cursor.execute(f"DELETE FROM crossmatch_results WHERE id_pncp IN ({','.join(['?']*len(ids_pncp))})", ids_pncp)
    
    df_db.to_sql("crossmatch_results", conn, if_exists="append", index=False, method="multi")
    conn.commit()
    conn.close()
    logger.info(f"{len(df_db)} resultados de crossmatch salvos no banco para {municipio}.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    inicializar_banco()

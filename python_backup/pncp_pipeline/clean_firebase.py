import logging
from firebase_admin import credentials, firestore, initialize_app, get_app
from pathlib import Path

# Configuração de Logs
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

COLECAO_MUNICIPIOS = "municipios"
SUB_COLECOES = ["apenas_pncp", "ambos", "apenas_aplic"]

CIDADES_FOCO_SLUGS = [
    "acorizal",
    "jangada",
    "lucas_do_rio_verde",
    "rondolandia",
    "sinop"
]

def get_firebase_db():
    try:
        app = get_app()
    except ValueError:
        cred_path = Path(__file__).parent.parent / "firebase_credentials.json"
        if not cred_path.exists():
            cred_path = Path(__file__).parent / "firebase_credentials.json"
        
        if not cred_path.exists():
            raise FileNotFoundError(f"Credenciais não encontradas em {cred_path}")
            
        cred = credentials.Certificate(str(cred_path))
        app = initialize_app(cred)
    return firestore.client()

def delete_collection(coll_ref, batch_size, db):
    """Deletes a collection in batches."""
    docs = coll_ref.limit(batch_size).stream()
    deleted = 0
    batch = db.batch()

    for doc in docs:
        batch.delete(doc.reference)
        deleted = deleted + 1

    if deleted > 0:
        batch.commit()
        return delete_collection(coll_ref, batch_size, db)

def clean_firebase():
    db = get_firebase_db()
    
    municipios_ref = db.collection(COLECAO_MUNICIPIOS)
    municipios = municipios_ref.stream()
    
    deleted_count = 0
    kept_count = 0
    
    for mun in municipios:
        mun_id = mun.id
        
        if mun_id not in CIDADES_FOCO_SLUGS:
            logger.info(f"Removendo município: {mun_id}")
            
            # Deletar subcoleções
            for sub_col_name in SUB_COLECOES:
                sub_col_ref = municipios_ref.document(mun_id).collection(sub_col_name)
                delete_collection(sub_col_ref, 500, db)
            
            # Deletar o documento pai
            mun.reference.delete()
            deleted_count += 1
        else:
            logger.info(f"Mantendo município foco: {mun_id}")
            kept_count += 1
            
    logger.info(f"Limpeza concluída. {deleted_count} municípios removidos. {kept_count} mantidos.")

if __name__ == "__main__":
    clean_firebase()

import os
from google.cloud import firestore
from dotenv import load_dotenv

load_dotenv()

def wipe_municipio(db, mun_slug):
    print(f"Limpando município: {mun_slug}")
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
            print(f"  - {col_name}: {deleted} documentos removidos.")

if __name__ == "__main__":
    db = firestore.Client.from_service_account_json("firebase_credentials.json")
    
    municipios = ["sinop", "lucas_do_rio_verde", "jangada", "rondonopolis", "acorizal", "rondolandia"]
    
    for mun in municipios:
        wipe_municipio(db, mun)
    
    print("Limpeza concluída.")

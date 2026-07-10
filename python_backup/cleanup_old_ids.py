import firebase_admin
from firebase_admin import credentials, firestore
import os
from dotenv import load_dotenv

load_dotenv()

cred_path = os.getenv('FIREBASE_CREDENTIALS_PATH') or 'firebase_credentials.json'
cred = credentials.Certificate(cred_path)
if not firebase_admin._apps:
    firebase_admin.initialize_app(cred)
db = firestore.client()

mun = 'acorizal'
collections = ['ambos', 'apenas_aplic']

print(f"--- Limpando IDs antigos para {mun} ---")
for coll_name in collections:
    docs = db.collection('municipios').document(mun).collection(coll_name).get()
    deleted = 0
    for doc in docs:
        # O novo ID tem 3 hifens (cnpj-numero-ano-mod) ou é o ID do PNCP (longo)
        # O ID antigo tinha apenas 2 hifens (cnpj-numero-ano)
        parts = doc.id.split('-')
        if len(parts) == 3: # Formato antigo: cnpj-numero-ano
            db.collection('municipios').document(mun).collection(coll_name).document(doc.id).delete()
            deleted += 1
    print(f"Coleção '{coll_name}': {deleted} documentos antigos removidos")

print("Limpeza concluída.")

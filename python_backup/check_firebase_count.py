import firebase_admin
from firebase_admin import credentials, firestore
import os
import json
from dotenv import load_dotenv

load_dotenv()

cred_path = os.getenv('FIREBASE_CREDENTIALS_PATH')
if not cred_path:
    # Tenta encontrar no root
    cred_path = 'firebase_credentials.json'

cred = credentials.Certificate(cred_path)
firebase_admin.initialize_app(cred)
db = firestore.client()

mun = 'acorizal'
collections = ['ambos', 'apenas_aplic']
total = 0

print(f"--- Verificando Firebase para {mun} ---")
for coll in collections:
    docs = db.collection('municipios').document(mun).collection(coll).get()
    count = len(docs)
    print(f"Coleção '{coll}': {count} documentos")
    total += count

print(f"Total APLIC no Firebase: {total}")

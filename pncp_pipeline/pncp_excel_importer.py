import pandas as pd
import json
import re
import requests
import sqlite3
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

EXCEL_PATH = r'C:\Users\ericguerrize\Desktop\pncp\pncpExtracaoDireta.xlsx'
DB_PATH = 'pncp_pipeline/monitor_pncp.db'
API_BASE = 'https://pncp.gov.br/api/consulta/v1'

def extract_info(id_str):
    try:
        if 'hyperlink' in id_str:
            url = json.loads(id_str)['hyperlink']
        else:
            url = id_str
        match = re.search(r'editais/(\d+)/(\d+)/(\d+)', url)
        if match:
            return match.groups()
    except Exception as e:
        pass
    return None, None, None

def fetch_pncp_detail(args):
    idx, row = args
    id_raw = row['Id da Contratação']
    cnpj, ano, seq = extract_info(id_raw)
    
    if not (cnpj and ano and seq):
        return None
        
    url = f"{API_BASE}/orgaos/{cnpj}/compras/{ano}/{sequencial if 'sequencial' in locals() else seq}"
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code == 200:
            return (idx, cnpj, ano, seq, resp.json())
    except:
        pass
    return None

def main():
    print(f"Lendo Excel: {EXCEL_PATH}")
    df = pd.read_excel(EXCEL_PATH)
    print(f"Encontrados {len(df)} registros.")

    conn = sqlite3.connect(DB_PATH)
    print("Limpando dados PNCP de 2025 no banco local...")
    conn.execute("DELETE FROM pncp_data WHERE ano = '2025'")
    conn.commit()
    
    print("Iniciando busca paralela na API...")
    tasks = list(df.iterrows())
    
    success_count = 0
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(fetch_pncp_detail, tasks))
        
        for res in results:
            if res:
                idx, cnpj, ano, seq, detail = res
                record_id = f"{cnpj}-{ano}-{seq}"
                municipio = detail.get('municipioNome', '')
                orgao = detail.get('orgaoEntidade', {}).get('razaoSocial', '')
                modalidade = detail.get('modalidadeNome', '')
                numero = detail.get('numeroCompra', '')
                objeto = detail.get('objetoCompra', '')
                valor = float(detail.get('valorTotalEstimado', 0))
                data_pub = detail.get('dataPublicacaoPncp', '')[:10]
                
                conn.execute("""
                    INSERT OR REPLACE INTO pncp_data 
                    (id, municipio, orgao, cnpj, modalidade, numero, ano, objeto, valor, data_publicacao, raw_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    record_id, municipio, orgao, cnpj, modalidade, numero, ano, objeto, valor, data_pub, json.dumps(detail)
                ))
                success_count += 1
                
    conn.commit()
    conn.close()
    print(f"\nFinalizado! {success_count} registros importados com sucesso.")

if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()

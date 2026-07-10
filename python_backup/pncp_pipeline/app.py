import subprocess
from flask import Flask, jsonify, request
from flask_cors import CORS
import os
import sys
import asyncio
import httpx
import pandas as pd
import numpy as np
import re

# Ensure the sibling modules are importable
sys.path.insert(0, os.path.dirname(__file__))

from aplic_extractor import descobrir_ugs, extrair_aplic
from crossmatch import crossmatch, load_orgaos
from normalizer import flatten_dict

app = Flask(__name__)
CORS(app) # Allow all origins so frontend can call it directly

# Helper to run async tasks in synchronous Flask handlers
def run_async(coro):
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)

async def fetch_cnpj_modality_page(client: httpx.AsyncClient, semaphore: asyncio.Semaphore, cnpj: str, ano: int, mod: int, pagina: int):
    base_url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
    data_inicial = f"{ano}0101"
    data_final = f"{ano}1231"
    params = {
        "dataInicial": data_inicial,
        "dataFinal": data_final,
        "cnpj": cnpj,
        "codigoModalidadeContratacao": mod,
        "uf": "MT",
        "pagina": pagina,
        "tamanhoPagina": 50
    }
    async with semaphore:
        resp = await client.get(base_url, params=params)
        if resp.status_code == 204:
            return {"data": [], "totalPaginas": 1}
        resp.raise_for_status()
        return resp.json()

async def fetch_pncp_by_cnpj_async(cnpj: str, ano: int, semaphore: asyncio.Semaphore, client: httpx.AsyncClient):
    modalidades = list(range(1, 14)) # 1 to 13
    
    async def fetch_modality(mod):
        results = []
        pagina = 1
        while True:
            try:
                data = await fetch_cnpj_modality_page(client, semaphore, cnpj, ano, mod, pagina)
                publications = data.get("data", [])
                
                # Print debug info
                print(f"[Debug PNCP] CNPJ {cnpj}, mod {mod}, pg {pagina} -> Encontrados: {len(publications)}")
                
                if not publications:
                    break
                results.extend(publications)
                
                total_paginas = data.get("totalPaginas", 1)
                if pagina >= total_paginas:
                    break
                pagina += 1
            except Exception as e:
                print(f"[PNCP API] Erro ao buscar CNPJ {cnpj}, modalidade {mod}, pagina {pagina}: {e}")
                break
        return results

    tasks = [fetch_modality(mod) for mod in modalidades]
    mod_results = await asyncio.gather(*tasks)
    
    flat_results = []
    for r in mod_results:
        flat_results.extend(r)
    return flat_results

async def fetch_all_pncp(cnpjs: list[str], ano: int):
    # Concurrency limit to prevent overloading/rate limiting
    semaphore = asyncio.Semaphore(15)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*"
    }
    # Create one shared client for all queries
    async with httpx.AsyncClient(timeout=30.0, headers=headers) as client:
        tasks = [fetch_pncp_by_cnpj_async(cnpj, ano, semaphore, client) for cnpj in cnpjs]
        results = await asyncio.gather(*tasks)
    
    flat_results = []
    for r in results:
        flat_results.extend(r)
    return flat_results

def serialize_df(df: pd.DataFrame) -> list[dict]:
    if df.empty:
        return []
    
    df_clean = df.copy()
    for col in df_clean.columns:
        if pd.api.types.is_datetime64_any_dtype(df_clean[col]):
            df_clean[col] = df_clean[col].dt.strftime('%Y-%m-%d')
        else:
            df_clean[col] = df_clean[col].apply(
                lambda x: x.strftime('%Y-%m-%d') if hasattr(x, 'strftime') else x
            )
            
    records = df_clean.to_dict(orient='records')
    for r in records:
        for k, v in r.items():
            if pd.isna(v) or v is np.nan:
                r[k] = None
    return records

@app.route('/sync/<municipio>', methods=['GET', 'POST'])
def sync_pipeline(municipio):
    pipeline_script = os.path.join(os.path.dirname(__file__), 'pipeline_multicidades.py')
    command = [sys.executable, pipeline_script, '--cidades', municipio, '--ano', '2026']

    print(f"Executando sync via API para: {municipio}")
    try:
        result = subprocess.run(command, capture_output=True, text=True, cwd=os.path.dirname(__file__))
        if result.returncode != 0:
            return jsonify({'status': 'error', 'message': f"Pipeline crash:\n{result.stderr}"}), 500
        
        return jsonify({
            'status': 'success', 
            'message': f"Pipeline para {municipio} rodou e enviou para o Firebase com sucesso!",
            'logs': result.stdout
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/live-crossmatch', methods=['GET'])
def live_crossmatch():
    municipio = request.args.get('municipio', '').strip()
    ano = request.args.get('ano', default=2026, type=int)
    
    if not municipio:
        return jsonify({'status': 'error', 'message': 'Parâmetro "municipio" é obrigatório.'}), 400
        
    print(f"\n[Live API] Cruzamento solicitado para: {municipio} (ano: {ano})")
    
    try:
        # 1. Descoberta de UGs e CNPJs no Oracle
        print(f"[Live API] Descobrindo UGs no Oracle...")
        df_ugs = descobrir_ugs([municipio])
        if df_ugs.empty:
            return jsonify({
                'status': 'error', 
                'message': f'Nenhum órgão/UG encontrado para "{municipio}" no banco Oracle.'
            }), 404
            
        ugs = df_ugs['ug_code'].tolist()
        df_ugs['cnpj'] = df_ugs['cnpj_publico'].fillna('').astype(str).str.replace(r'\D', '', regex=True)
        cnpjs = df_ugs['cnpj'].dropna().unique().tolist()
        cnpjs = [c for c in cnpjs if len(c) >= 14]
        
        print(f"[Live API] UGs encontradas: {ugs}")
        print(f"[Live API] CNPJs das UGs: {cnpjs}")
        
        # 2. Buscar APLIC do Oracle
        print(f"[Live API] Extraindo APLIC do Oracle...")
        df_aplic = extrair_aplic(ugs, ano)
        print(f"[Live API] Registros APLIC obtidos: {len(df_aplic)}")
        
        # 3. Buscar PNCP da API
        raw_pncp = []
        if cnpjs:
            print(f"[Live API] Buscando dados do PNCP...")
            raw_pncp = run_async(fetch_all_pncp(cnpjs, ano))
            print(f"[Live API] Registros PNCP crus obtidos: {len(raw_pncp)}")
            
        # Normalizar e criar df_pncp
        df_pncp = pd.DataFrame()
        if raw_pncp:
            normalized_pncp = []
            for item in raw_pncp:
                flat_item = flatten_dict(item)
                normalized_pncp.append(flat_item)
            df_pncp = pd.DataFrame(normalized_pncp)
            
            # Limpeza do dataset
            from dataset_builder import clean_dataset
            df_pncp = clean_dataset(df_pncp)
            
        print(f"[Live API] Registros PNCP após limpeza: {len(df_pncp)}")
        
        # 4. Executar Crossmatch
        print("[Live API] Executando crossmatch...")
        load_orgaos()
        
        df_resultado, _ = crossmatch(df_pncp, df_aplic)
        print(f"[Live API] Total registros crossmatch: {len(df_resultado)}")
        
        # 5. Segmentar resultados
        ambos = pd.DataFrame()
        apenas_pncp = pd.DataFrame()
        apenas_aplic = pd.DataFrame()
        
        if not df_resultado.empty:
            status_col = 'status_cruzamento' if 'status_cruzamento' in df_resultado.columns else None
            if status_col:
                ambos = df_resultado[df_resultado[status_col].isin(['MATCH_CONFIRMADO', 'MATCH_PARCIAL'])]
                apenas_pncp = df_resultado[df_resultado[status_col] == 'SEM_MATCH']
                apenas_aplic = df_resultado[df_resultado[status_col] == 'APENAS_APLIC']
            else:
                ambos = df_resultado
                
        # 6. Serializar
        ambos_records = serialize_df(ambos)
        apenas_pncp_records = serialize_df(apenas_pncp)
        apenas_aplic_records = serialize_df(apenas_aplic)
        
        ugs_list = df_ugs[['ug_code', 'nome', 'cnpj']].to_dict(orient='records')
        
        return jsonify({
            'status': 'success',
            'municipio': municipio,
            'ano': ano,
            'ugs': ugs_list,
            'stats': {
                'ambos': len(ambos_records),
                'apenas_pncp': len(apenas_pncp_records),
                'apenas_aplic': len(apenas_aplic_records)
            },
            'data': {
                'ambos': ambos_records,
                'apenas_pncp': apenas_pncp_records,
                'apenas_aplic': apenas_aplic_records
            }
        })
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    print("Iniciando Servidor Flask na porta 5000...")
    print("Endpoint de sync: GET http://localhost:5000/sync/<municipio>")
    print("Endpoint de cruzamento ao vivo: GET http://localhost:5000/api/live-crossmatch?municipio=cuiaba&ano=2026")
    app.run(host='0.0.0.0', port=5000, debug=True)


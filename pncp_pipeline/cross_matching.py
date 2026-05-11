# cross_matching.py
"""
CROSS MATCHING APLIC vs PNCP

Entrada: 
  - aplic_extract.csv (extraído pelo usuário do Oracle)
  - pncp_extract.csv (gerado pelo pncp_extractor.py)

Saída: cross_resultado.xlsx (com 6 abas)

Uso:
    python cross_matching.py
"""

import pandas as pd
import os
import sys
from datetime import datetime
from difflib import SequenceMatcher

print("=" * 100)
print("CROSS MATCHING APLIC vs PNCP")
print("=" * 100)

# ========== VALIDAR ARQUIVOS ==========

print("\n[1/6] Validando arquivos de entrada...")

if not os.path.exists('aplic_extract.csv'):
    print("\n✗ ERRO: aplic_extract.csv não encontrado!")
    print("\nSolução:")
    print("  1. Extraia APLIC do Oracle")
    print("  2. Salve como: aplic_extract.csv")
    print("  3. Coloque na mesma pasta que este script")
    exit(1)

if not os.path.exists('pncp_extract.csv'):
    print("\n✗ ERRO: pncp_extract.csv não encontrado!")
    print("\nSolução:")
    print("  1. Rode: python pncp_extractor.py")
    print("  2. Depois rode este script novamente")
    exit(1)

print("  ✓ aplic_extract.csv encontrado")
print("  ✓ pncp_extract.csv encontrado")

# ========== CARREGAR DADOS ==========

print("\n[2/6] Carregando dados...")

try:
    df_aplic = pd.read_csv('aplic_extract.csv', encoding='utf-8')
    df_pncp = pd.read_csv('pncp_extract.csv', encoding='utf-8')
    
    print(f"  ✓ APLIC: {len(df_aplic)} registros")
    print(f"  ✓ PNCP: {len(df_pncp)} registros")
except Exception as e:
    print(f"  ✗ Erro ao carregar: {e}")
    exit(1)

# ========== PREPARAR DADOS ==========

print("\n[3/6] Preparando dados...")

def normalizar_ug(ug):
    """Normaliza UG para comparação"""
    if not ug or pd.isna(ug):
        return ""
    ug = str(ug).upper().strip()
    # Simplificar
    if 'PREFEITURA' in ug or 'PREFEITO' in ug:
        return 'PREFEITURA'
    elif 'CAMARA' in ug or 'CÂMARA' in ug:
        return 'CAMARA'
    elif 'PREV' in ug:
        return 'PREVIDENCIA'
    return ug[:20]

def extrair_data(data_str):
    """Extrai data em formato comparável"""
    if not data_str or pd.isna(data_str):
        return None
    try:
        # Tenta formato DD/MM/YYYY
        return pd.to_datetime(data_str, format='%d/%m/%Y', errors='coerce')
    except:
        return None

def extrair_valor(val):
    """Extrai valor numérico"""
    if not val or pd.isna(val):
        return 0
    try:
        return float(val)
    except:
        return 0

# Preparar APLIC
aplic_prep = pd.DataFrame()
aplic_prep['id_aplic'] = range(len(df_aplic))
aplic_prep['ug'] = [normalizar_ug(ug) for ug in df_aplic.iloc[:, 1]]
aplic_prep['data'] = [extrair_data(d) for d in df_aplic.iloc[:, 2]]
aplic_prep['numero'] = df_aplic.iloc[:, 3].astype(str).str.strip() if len(df_aplic.columns) > 3 else ''
aplic_prep['modalidade'] = df_aplic.iloc[:, 4].astype(str).str.upper().str.strip() if len(df_aplic.columns) > 4 else ''
aplic_prep['valor'] = [extrair_valor(v) for v in df_aplic.iloc[:, 5]]
aplic_prep['objetivo'] = df_aplic.iloc[:, 6].astype(str).str.strip() if len(df_aplic.columns) > 6 else ''

# Preparar PNCP
pncp_prep = pd.DataFrame()
pncp_prep['id_pncp'] = df_pncp['ID'].values
pncp_prep['ug'] = [normalizar_ug(ug) for ug in df_pncp['Unidade']]
pncp_prep['data'] = [extrair_data(d) for d in df_pncp['Data']]
pncp_prep['modalidade'] = df_pncp['Modalidade'].astype(str).str.upper().str.strip()
pncp_prep['valor'] = df_pncp['Valor_Numérico'].values
pncp_prep['orgao'] = df_pncp['Órgão'].astype(str).str.strip()

print(f"  ✓ APLIC preparado")
print(f"  ✓ PNCP preparado")

# ========== FUNCAO DE MATCHING ==========

print("\n[4/6] Executando matching...")

def encontrar_match(aplic_row, pncp_list):
    """Encontra melhor match para APLIC em PNCP"""
    
    best_match_idx = None
    best_score = 0
    
    for pncp_idx, pncp_row in pncp_list.iterrows():
        score = 0
        
        # Critério 1: UG deve bater (SINOP em ambos)
        if aplic_row['ug'] != pncp_row['ug']:
            continue
        
        score += 40  # Base 40 pontos
        
        # Critério 2: Data próxima (±5 dias)
        if pd.notna(aplic_row['data']) and pd.notna(pncp_row['data']):
            diff_dias = abs((aplic_row['data'] - pncp_row['data']).days)
            if diff_dias <= 5:
                score += 30
            elif diff_dias <= 15:
                score += 15
            else:
                continue  # Não bate
        
        # Critério 3: Valor similar (±10%)
        if aplic_row['valor'] > 0 and pncp_row['valor'] > 0:
            diff_pct = abs(aplic_row['valor'] - pncp_row['valor']) / aplic_row['valor'] * 100
            if diff_pct <= 10:
                score += 20
            elif diff_pct <= 20:
                score += 10
            else:
                continue  # Não bate
        
        # Critério 4: Modalidade (bonus)
        if aplic_row['modalidade'] and pncp_row['modalidade']:
            if aplic_row['modalidade'][:10] == pncp_row['modalidade'][:10]:
                score += 10
        
        if score > best_score:
            best_score = score
            best_match_idx = pncp_idx
    
    return best_match_idx, best_score

# Fazer matching
matches_list = []
for aplic_idx, aplic_row in aplic_prep.iterrows():
    match_idx, score = encontrar_match(aplic_row, pncp_prep)
    
    if match_idx is not None:
        matches_list.append({
            'id_aplic': aplic_idx,
            'id_pncp': match_idx,
            'score': score,
            'matched': True
        })
    else:
        matches_list.append({
            'id_aplic': aplic_idx,
            'id_pncp': None,
            'score': 0,
            'matched': False
        })

df_matches = pd.DataFrame(matches_list)

matches_encontrados = df_matches['matched'].sum()
print(f"  ✓ Matches encontrados: {matches_encontrados}/{len(aplic_prep)}")

# ========== CRIAR ABAS EXCEL ==========

print("\n[5/6] Gerando abas Excel...")

# Aba 1: APLIC Completo
aba1_aplic = df_aplic.copy()
aba1_aplic.insert(0, 'Match_PNCP', 
                  [df_matches.loc[i, 'id_pncp'] if df_matches.loc[i, 'matched'] else 'SEM MATCH' 
                   for i in range(len(df_aplic))])

# Aba 2: PNCP Completo
aba2_pncp = df_pncp.copy()
aba2_pncp.insert(0, 'Match_APLIC',
                 ['SEM MATCH'] * len(df_pncp))  # Será preenchido depois

for aplic_id, pncp_id in zip(df_matches[df_matches['matched']]['id_aplic'],
                              df_matches[df_matches['matched']]['id_pncp']):
    aba2_pncp.loc[pncp_id, 'Match_APLIC'] = aplic_id

# Aba 3: Matches
matches_detail = []
for _, match in df_matches[df_matches['matched']].iterrows():
    aplic_idx = int(match['id_aplic'])
    pncp_idx = int(match['id_pncp'])
    
    matches_detail.append({
        'ID_APLIC': aplic_idx,
        'Data_APLIC': aplic_prep.loc[aplic_idx, 'data'].strftime('%d/%m/%Y') if pd.notna(aplic_prep.loc[aplic_idx, 'data']) else 'N/A',
        'UG_APLIC': aplic_prep.loc[aplic_idx, 'ug'],
        'Número_APLIC': aplic_prep.loc[aplic_idx, 'numero'],
        'Modalidade_APLIC': aplic_prep.loc[aplic_idx, 'modalidade'],
        'Valor_APLIC': f"{aplic_prep.loc[aplic_idx, 'valor']:,.2f}" if aplic_prep.loc[aplic_idx, 'valor'] > 0 else 'N/A',
        
        'ID_PNCP': pncp_idx,
        'Data_PNCP': pncp_prep.loc[pncp_idx, 'data'].strftime('%d/%m/%Y') if pd.notna(pncp_prep.loc[pncp_idx, 'data']) else 'N/A',
        'UG_PNCP': pncp_prep.loc[pncp_idx, 'ug'],
        'Modalidade_PNCP': pncp_prep.loc[pncp_idx, 'modalidade'],
        'Valor_PNCP': f"{pncp_prep.loc[pncp_idx, 'valor']:,.2f}" if pncp_prep.loc[pncp_idx, 'valor'] > 0 else 'N/A',
        
        'Score': f"{match['score']:.0f}"
    })

aba3_matches = pd.DataFrame(matches_detail)

# Aba 4: Apenas APLIC (sem par em PNCP)
apenas_aplic_ids = df_matches[~df_matches['matched']]['id_aplic'].values
aba4_apenas_aplic = aplic_prep.loc[apenas_aplic_ids].copy()
aba4_apenas_aplic = aba4_apenas_aplic[[col for col in aba4_apenas_aplic.columns if col != 'id_aplic']]
aba4_apenas_aplic.insert(0, 'ID_APLIC', apenas_aplic_ids)

# Aba 5: Apenas PNCP (sem par em APLIC)
matched_pncp = set(df_matches[df_matches['matched']]['id_pncp'].dropna().astype(int))
apenas_pncp_ids = [i for i in pncp_prep.index if i not in matched_pncp]
aba5_apenas_pncp = pncp_prep.loc[apenas_pncp_ids].copy()
aba5_apenas_pncp = aba5_apenas_pncp[[col for col in aba5_apenas_pncp.columns if col != 'id_pncp']]
aba5_apenas_pncp.insert(0, 'ID_PNCP', [pncp_prep.loc[i, 'id_pncp'] for i in apenas_pncp_ids])

# Aba 6: Resumo
resumo_data = {
    'Métrica': [
        'Total APLIC',
        'Total PNCP',
        'Total Registros Únicos',
        '',
        'Matches encontrados',
        'Apenas APLIC (gap PNCP)',
        'Apenas PNCP (gap APLIC)',
        '',
        'Taxa de Cobertura APLIC',
        'Taxa de Cobertura PNCP',
    ],
    'Valor': [
        len(aplic_prep),
        len(pncp_prep),
        len(aplic_prep) + len(apenas_pncp_ids),
        '',
        matches_encontrados,
        len(apenas_aplic_ids),
        len(apenas_pncp_ids),
        '',
        f"{matches_encontrados/len(aplic_prep)*100:.1f}%",
        f"{matches_encontrados/len(pncp_prep)*100:.1f}%",
    ]
}
aba6_resumo = pd.DataFrame(resumo_data)

print(f"  ✓ Aba APLIC_Completo: {len(aba1_aplic)} linhas")
print(f"  ✓ Aba PNCP_Completo: {len(aba2_pncp)} linhas")
print(f"  ✓ Aba MATCHES: {len(aba3_matches)} linhas")
print(f"  ✓ Aba Apenas_APLIC: {len(aba4_apenas_aplic)} linhas")
print(f"  ✓ Aba Apenas_PNCP: {len(aba5_apenas_pncp)} linhas")
print(f"  ✓ Aba Resumo: {len(aba6_resumo)} linhas")

# ========== EXPORTAR EXCEL ==========

print("\n[6/6] Exportando Excel...")

try:
    with pd.ExcelWriter('cross_resultado.xlsx', engine='openpyxl') as writer:
        aba1_aplic.to_excel(writer, sheet_name='APLIC_Completo', index=False)
        aba2_pncp.to_excel(writer, sheet_name='PNCP_Completo', index=False)
        aba3_matches.to_excel(writer, sheet_name='MATCHES', index=False)
        aba4_apenas_aplic.to_excel(writer, sheet_name='Apenas_APLIC', index=False)
        aba5_apenas_pncp.to_excel(writer, sheet_name='Apenas_PNCP', index=False)
        aba6_resumo.to_excel(writer, sheet_name='Resumo', index=False)
    
    print(f"  ✓ Arquivo criado: cross_resultado.xlsx")
except Exception as e:
    print(f"  ✗ Erro ao exportar: {e}")
    exit(1)

# ========== RELATORIO FINAL ==========

print("\n" + "=" * 100)
print("✓ CROSS MATCHING CONCLUIDO COM SUCESSO!")
print("=" * 100)

print(f"""
RESULTADO:
├─ Total APLIC: {len(aplic_prep)}
├─ Total PNCP: {len(pncp_prep)}
│
├─ Matches encontrados: {matches_encontrados} ({matches_encontrados/len(aplic_prep)*100:.1f}%)
├─ Apenas APLIC (gap PNCP): {len(apenas_aplic_ids)} ({len(apenas_aplic_ids)/len(aplic_prep)*100:.1f}%)
├─ Apenas PNCP (gap APLIC): {len(apenas_pncp_ids)} ({len(apenas_pncp_ids)/len(pncp_prep)*100:.1f}%)
│
└─ ARQUIVO GERADO: cross_resultado.xlsx

ABAS CRIADAS:
├─ 1. APLIC_Completo → Todos os {len(aplic_prep)} APLIC
├─ 2. PNCP_Completo → Todos os {len(pncp_prep)} PNCP
├─ 3. MATCHES → {matches_encontrados} matches encontrados
├─ 4. Apenas_APLIC → {len(apenas_aplic_ids)} registros sem par em PNCP
├─ 5. Apenas_PNCP → {len(apenas_pncp_ids)} registros sem par em APLIC
└─ 6. Resumo → Estatísticas consolidadas

PRÓXIMO PASSO:
├─ Abra: cross_resultado.xlsx
├─ Revise cada aba
└─ Analise gaps (Apenas_APLIC e Apenas_PNCP)
""")

print("=" * 100)

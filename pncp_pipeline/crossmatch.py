"""
crossmatch.py — Cruzamento PNCP × APLIC (TCE-MT)

Cascata de matching (ordem de prioridade):
  Tier 1 — Semântico + Financeiro: fuzzy objeto/objetivo (≥85) + valor delta ≤10%
  Tier 2 — CNPJ + Data: CNPJ exato + abertura vs publicação ≤30 dias
  Tier 3 — Estrutural (fallback): município + número + ano + modalidade

Score composto pós-match:
  50% fuzzy texto + 30% delta valor + 20% delta data
  ≥85 → MATCH_CONFIRMADO | ≥70 → MATCH_PARCIAL | <70 → SEM_MATCH (rebaixado)

Uso standalone:
    python crossmatch.py pncp_contratacoes_MT_*.xlsx licitacao_lrv_2026.csv
"""

import re
import sys
import csv
import logging
import unicodedata
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

# DE-PARA: (cod_ug, municipio_normalizado) → CNPJ (14 dígitos sem formatação)
DE_PARA_UG_CNPJ: dict[tuple[str, str], str] = {
    ("1111319", "lucas do rio verde"): "24772246000140",
    ("1113125", "cuiaba"):             "03533064000146",
    ("1118736", "sinop"):              "00814574000101",  # Câmara Municipal de Sinop
    ("1113257", "sinop"):              "00571071000144",  # Instituto de Previdência de Sinop
    ("1112309", "sinop"):              "15024003000132",  # Prefeitura Municipal de Sinop
}

# Modalidade APLIC (código string) → modalidadeId PNCP (int)
MAPA_MODALIDADE_APLIC_PARA_PNCP: dict[str, int] = {
    "08": 8,   # Dispensa de Licitação
    "09": 9,   # Inexigibilidade
    "13": 6,   # Pregão Eletrônico
    "15": 12,  # Credenciamento
    "56": 5,   # Concorrência Presencial
    "01": 4,   # Concorrência Eletrônica
    "02": 2,   # Diálogo Competitivo
    "03": 3,   # Concurso
    "04": 1,   # Leilão Eletrônico
    "05": 7,   # Pregão Presencial
    "06": 10,  # Manifestação de Interesse
    "07": 11,  # Pré-qualificação
    "14": 13,  # Leilão Presencial
}

LIMIAR_MATCH_CONFIRMADO = 85
LIMIAR_MATCH_PARCIAL    = 70

# Tier 1: semântico + financeiro
LIMIAR_FUZZY_T1 = 85    # token_sort_ratio mínimo
LIMIAR_VALOR_T1 = 10.0  # delta valor % máximo

# Tier 2: CNPJ + data
LIMIAR_DIAS_T2 = 30     # diferença máxima em dias


# ---------------------------------------------------------------------------
# Funções utilitárias
# ---------------------------------------------------------------------------

def extrair_numero_puro(texto: str) -> tuple[int | None, int | None]:
    """
    Extrai número base e ano de qualquer formato de número de licitação.

    Exemplos:
        "00000000001/2026"  → (1, 2026)
        "PE 8"              → (8, None)
        "021"               → (21, None)
        "011/2025/PMC"      → (11, 2025)
        "DL 2"              → (2, None)
    """
    if not isinstance(texto, str) or not texto.strip():
        return None, None

    texto = texto.strip()

    # Tenta padrão: número/ano (captura o primeiro par número/ano)
    m = re.search(r'(\d+)\s*/\s*(20\d{2}|\d{2})', texto)
    if m:
        numero = int(m.group(1))
        ano_raw = m.group(2)
        ano = int(ano_raw) if len(ano_raw) == 4 else 2000 + int(ano_raw)
        return numero, ano

    # Tenta extrair apenas número inteiro (ignora prefixos alfanuméricos)
    m = re.search(r'(\d+)', texto)
    if m:
        return int(m.group(1)), None

    return None, None


def normalizar_texto(texto: str) -> str:
    """
    Normaliza texto: lowercase + remove acentos + expurga stopwords licitatórias.
    """
    if not isinstance(texto, str):
        return ""
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(c for c in texto if not unicodedata.combining(c))
    texto = texto.lower()
    
    stopwords = [
        r"registro de precos para( a| o)?( contratacao de empresa especializada (para|no|na)| fornecimento de| eventual e futura contratacao de empresa especializada na prestacao de servicos continuados de| futura e eventual aquisicao de)?",
        r"registro de precos( para)?",
        r"aquisicao de( agregados para a construcao civil substancia mineral de rocha britada em diversos tipos e granulometrias para utilizacao em manutencoes e reparos em vias urbanas e rurais pavimentacao drenagem e obras de arte bem como para execucao de| materiais e insumos para( equipar as)?| kits de teste de)?",
        r"contratacao( de empresa( especializada( na prestacao de| para( o fornecimento de equipamentos para)?| em servicos de)?| por notoria especializacao para a)?| artistica mediante inexigibilidade para realizacao de)?",
        r"prestacao de servicos?",
        r"fornecimento( e instalacao)? de",
        r"futura e eventual",
        r"eventual e futura",
        r"credenciamento( destinado a empresas que tenham interesse na prestacao de servicos especializados na realizacao de| de pessoas juridicas para a prestacao de servicos medicos destinados ao atendimento na| de empresa especializada em prestacao de)?",
    ]
    padrao = r"\b(" + "|".join(stopwords) + r")\b"
    texto = re.sub(padrao, " ", texto)
    
    return re.sub(r'\s+', ' ', texto).strip()


def converter_valor_br(texto) -> float | None:
    """
    Converte valor monetário brasileiro para float.

    Exemplos:
        "1.234,56" → 1234.56
        "1234.56"  → 1234.56
        1234       → 1234.0
        ""         → None
    """
    if texto is None:
        return None
    if isinstance(texto, (int, float)):
        return float(texto)
    texto = str(texto).strip()
    if not texto:
        return None

    # Formato BR: "1.234,56"
    if re.match(r'^\d{1,3}(\.\d{3})*(,\d+)?$', texto):
        texto = texto.replace('.', '').replace(',', '.')
        try:
            return float(texto)
        except ValueError:
            pass

    # Formato anglo-saxão ou inteiro puro
    texto_limpo = re.sub(r'[^\d.]', '', texto)
    try:
        return float(texto_limpo)
    except ValueError:
        return None


def mapear_ug_para_cnpj(cod_ug, municipio: str) -> str | None:
    """Lookup no DE-PARA via (cod_ug, municipio_normalizado)."""
    if not cod_ug or str(cod_ug).strip() == '':
        return None
    chave = (str(cod_ug).strip(), normalizar_texto(municipio))
    return DE_PARA_UG_CNPJ.get(chave)


def mapear_modalidade_aplic_para_pncp(cod_mod: str) -> int | None:
    """Lookup no mapa de modalidades APLIC → PNCP."""
    if not cod_mod:
        return None
    return MAPA_MODALIDADE_APLIC_PARA_PNCP.get(str(cod_mod).strip().zfill(2))


# ---------------------------------------------------------------------------
# Preparação dos DataFrames
# ---------------------------------------------------------------------------

def preparar_pncp(df_pncp: pd.DataFrame) -> pd.DataFrame:
    """Prepara o DataFrame PNCP para o cruzamento."""
    df = df_pncp.copy()

    # Extrai número puro e ano do numeroCompra
    parsed = df['numeroCompra'].astype(str).apply(extrair_numero_puro)
    df['_numero_puro'] = parsed.apply(lambda x: x[0])
    df['_ano_extraido'] = parsed.apply(lambda x: x[1])

    # Fallback: usa anoCompra quando ano não está no numeroCompra
    if 'anoCompra' in df.columns:
        mask_sem_ano = df['_ano_extraido'].isna()
        df.loc[mask_sem_ano, '_ano_extraido'] = pd.to_numeric(
            df.loc[mask_sem_ano, 'anoCompra'], errors='coerce'
        )

    # Normaliza município e objeto
    df['_municipio_norm'] = df['unidadeOrgao_municipioNome'].apply(normalizar_texto)
    df['_objeto_norm'] = df['objetoCompra'].apply(normalizar_texto)

    # Garante valores financeiros como float
    for col in ['valorTotalEstimado', 'valorTotalHomologado']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

    # dataPublicacaoPncp → datetime
    if 'dataPublicacaoPncp' in df.columns:
        df['dataPublicacaoPncp'] = pd.to_datetime(df['dataPublicacaoPncp'], errors='coerce')

    # Remove formatação de CNPJ
    if 'orgaoEntidade_cnpj' in df.columns:
        df['orgaoEntidade_cnpj'] = df['orgaoEntidade_cnpj'].astype(str).apply(
            lambda x: re.sub(r'\D', '', x)
        )

    # modalidadeId como float64 para joins sem problemas de tipo
    if 'modalidadeId' in df.columns:
        df['modalidadeId'] = pd.to_numeric(df['modalidadeId'], errors='coerce').astype('float64')

    return df


def preparar_aplic(df_aplic: pd.DataFrame) -> pd.DataFrame:
    """Prepara o DataFrame APLIC para o cruzamento."""
    df = df_aplic.copy()

    # Forward-fill do Cód. UG (aparece apenas na primeira linha de cada grupo)
    col_ug = next((c for c in df.columns if 'ug' in c.lower() and ('cód' in c.lower() or 'cod' in c.lower())), None)
    if col_ug:
        df[col_ug] = df[col_ug].replace('', pd.NA).ffill()

    # Coluna de número da licitação
    col_numero = next(
        (c for c in df.columns if 'licitaç' in c.lower() or 'licitac' in c.lower() or 'nº' in c.lower()),
        None
    )
    if col_numero is None:
        col_numero = df.columns[0]
        logger.warning(f"Coluna de número não encontrada; usando '{col_numero}'")

    parsed = df[col_numero].astype(str).apply(extrair_numero_puro)
    df['_numero_puro'] = parsed.apply(lambda x: x[0])
    df['_ano_extraido'] = parsed.apply(lambda x: x[1])

    # Fallback: usa coluna Exercício
    col_exercicio = next((c for c in df.columns if 'exerc' in c.lower()), None)
    if col_exercicio:
        mask_sem_ano = df['_ano_extraido'].isna()
        df.loc[mask_sem_ano, '_ano_extraido'] = pd.to_numeric(
            df.loc[mask_sem_ano, col_exercicio], errors='coerce'
        )

    # Mapeia UG → CNPJ
    # Prefere coluna "Município" pura, evita "Cód. município"
    col_municipio = next(
        (c for c in df.columns if c.strip().lower() == 'município'), None
    ) or next(
        (c for c in df.columns if 'munic' in c.lower() and 'cód' not in c.lower() and 'cod' not in c.lower()),
        None
    )
    if col_ug and col_municipio:
        df['_cnpj_mapeado'] = df.apply(
            lambda r: mapear_ug_para_cnpj(r[col_ug], r[col_municipio]), axis=1
        )
    else:
        df['_cnpj_mapeado'] = None
        logger.warning("Colunas Cód. UG ou Município não encontradas no APLIC.")

    # Mapeia modalidade → float64 para join sem problemas de tipo
    col_modalidade = next((c for c in df.columns if 'modalidade' in c.lower() and 'cod' in c.lower()), None)
    if col_modalidade:
        df['_modalidade_pncp_id'] = pd.to_numeric(
            df[col_modalidade].astype(str).apply(mapear_modalidade_aplic_para_pncp),
            errors='coerce'
        ).astype('float64')
    else:
        df['_modalidade_pncp_id'] = pd.NA
        logger.warning("Coluna Cod. Modalidade não encontrada no APLIC.")

    # Normaliza município e objetivo
    if col_municipio:
        df['_municipio_norm'] = df[col_municipio].apply(normalizar_texto)
    else:
        df['_municipio_norm'] = ""
        logger.warning("Coluna Município não encontrada no APLIC.")

    col_objetivo = next((c for c in df.columns if 'objetivo' in c.lower() or 'objeto' in c.lower()), None)
    if col_objetivo:
        df['_objetivo_norm'] = df[col_objetivo].apply(normalizar_texto)
    else:
        df['_objetivo_norm'] = ""
        logger.warning("Coluna Objetivo não encontrada no APLIC.")

    # Valores financeiros
    col_val_est = next((c for c in df.columns if 'estimado' in c.lower()), None)
    col_val_venc = next((c for c in df.columns if 'vencedor' in c.lower()), None)
    df['_valor_estimado_float'] = df[col_val_est].apply(converter_valor_br) if col_val_est else None
    df['_valor_vencedor_float'] = df[col_val_venc].apply(converter_valor_br) if col_val_venc else None

    # Data Abertura
    col_data = next((c for c in df.columns if 'abertura' in c.lower()), None)
    if col_data:
        df[col_data] = pd.to_datetime(df[col_data], dayfirst=True, errors='coerce')

    return df


# ---------------------------------------------------------------------------
# Deduplicação
# ---------------------------------------------------------------------------

def deduplicar_pncp(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplica o DataFrame PNCP.
    Chave: (cnpj, número, ano, modalidade).
    Prioridade: valorTotalHomologado > 0 primeiro; empate → dataPublicacaoPncp mais recente.
    """
    antes = len(df)
    chave = ['orgaoEntidade_cnpj', '_numero_puro', '_ano_extraido', 'modalidadeId']
    chave_existente = [c for c in chave if c in df.columns]

    if not chave_existente:
        return df

    df = df.copy()
    if 'valorTotalHomologado' in df.columns:
        df['_tem_homologado'] = (pd.to_numeric(df['valorTotalHomologado'], errors='coerce').fillna(0) > 0).astype(int)
    else:
        df['_tem_homologado'] = 0
    df = df.sort_values(
        ['_tem_homologado', 'dataPublicacaoPncp'],
        ascending=[False, False],
        na_position='last'
    )
    df = df.drop_duplicates(subset=chave_existente, keep='first')
    df = df.drop(columns=['_tem_homologado'])

    removidos = antes - len(df)
    if removidos:
        logger.info(f"[PNCP] Deduplicação: {removidos} linhas removidas ({antes} → {len(df)})")
    return df


def deduplicar_aplic(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplica o DataFrame APLIC.
    Chave: (número, ano, modalidade_pncp).
    Prioridade: maior valor estimado.
    """
    antes = len(df)
    chave = ['_numero_puro', '_ano_extraido', '_modalidade_pncp_id']
    chave_existente = [c for c in chave if c in df.columns]

    if not chave_existente:
        return df

    df = df.copy()
    df = df.sort_values('_valor_estimado_float', ascending=False, na_position='last')
    df = df.drop_duplicates(subset=chave_existente, keep='first')

    removidos = antes - len(df)
    if removidos:
        logger.info(f"[APLIC] Deduplicação: {removidos} linhas removidas ({antes} → {len(df)})")
    return df


# ---------------------------------------------------------------------------
# Lógica de merge em cascata
# ---------------------------------------------------------------------------

def _merge_primario(
    df_pncp: pd.DataFrame,
    df_aplic: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, set]:
    """
    Tier 1 — Semântico + Financeiro (mais discriminante).
    Aceita match se fuzzy objeto/objetivo >= LIMIAR_FUZZY_T1
    E delta valor <= LIMIAR_VALOR_T1%.
    Agrupa por (municipio_norm, ano). Atribuição greedy (maior score primeiro).

    Retorna (matched, pncp_orfaos, matched_aplic_indices).
    """
    try:
        from rapidfuzz import process as rfprocess, fuzz as rffuzz
    except ImportError:
        logger.warning("rapidfuzz não instalado. Tier 1 (semântico) ignorado.")
        return pd.DataFrame(), df_pncp.copy(), set()

    matched_rows: list[dict] = []
    matched_pncp_idx: set = set()
    matched_aplic_idx: set = set()

    grupos = df_pncp.groupby(['_municipio_norm', '_ano_extraido'], dropna=False)

    for (mun, ano), grupo_pncp in grupos:
        grupo_aplic = df_aplic[
            (df_aplic['_municipio_norm'] == mun) &
            (df_aplic['_ano_extraido'] == ano)
        ]
        if grupo_aplic.empty:
            continue

        objetos   = grupo_pncp['_objeto_norm'].fillna('').tolist()
        objetivos = grupo_aplic['_objetivo_norm'].fillna('').tolist()
        matrix    = rfprocess.cdist(objetos, objetivos, scorer=rffuzz.token_sort_ratio)

        pncp_indices  = list(grupo_pncp.index)
        aplic_indices = list(grupo_aplic.index)

        # Monta candidatos: (score_texto, pncp_ri, aplic_ri)
        candidatos: list[tuple] = []
        for i, pncp_ri in enumerate(pncp_indices):
            for j, aplic_ri in enumerate(aplic_indices):
                score_texto = float(matrix[i][j])
                if score_texto < LIMIAR_FUZZY_T1:
                    continue
                # Verifica delta de valor
                val_p = pd.to_numeric(
                    df_pncp.loc[pncp_ri, 'valorTotalEstimado'], errors='coerce'
                ) or 0.0
                val_a = df_aplic.loc[aplic_ri, '_valor_estimado_float'] or 0.0
                if val_p > 0 and val_a > 0:
                    delta_val = abs(val_p - val_a) / max(val_p, val_a) * 100
                    if delta_val > LIMIAR_VALOR_T1:
                        continue
                candidatos.append((score_texto, pncp_ri, aplic_ri))

        # Atribuição greedy: maior score primeiro
        candidatos.sort(key=lambda x: -x[0])
        for score_t, pncp_ri, aplic_ri in candidatos:
            if pncp_ri in matched_pncp_idx or aplic_ri in matched_aplic_idx:
                continue
            row = df_pncp.loc[pncp_ri].to_dict()
            for col in df_aplic.columns:
                if col not in row:
                    row[col] = df_aplic.loc[aplic_ri, col]
            row['_origem_merge']      = 'primario_semantico'
            row['_fuzzy_score_tier1'] = score_t
            matched_rows.append(row)
            matched_pncp_idx.add(pncp_ri)
            matched_aplic_idx.add(aplic_ri)

    if not matched_rows:
        logger.info(f"[Tier 1] 0 matches | {len(df_pncp)} órfãos PNCP")
        return pd.DataFrame(), df_pncp.copy(), set()

    matched     = pd.DataFrame(matched_rows)
    pncp_orfaos = df_pncp[~df_pncp.index.isin(matched_pncp_idx)].copy()
    logger.info(f"[Tier 1] {len(matched)} matches | {len(pncp_orfaos)} órfãos PNCP")
    return matched, pncp_orfaos, matched_aplic_idx


def _merge_secundario(
    df_pncp_orfaos: pd.DataFrame,
    df_aplic: pd.DataFrame,
    already_matched_aplic: set,
) -> tuple[pd.DataFrame, pd.DataFrame, set]:
    """
    Tier 2 — CNPJ + Data.
    Para cada PNCP órfão cujo CNPJ está no DE-PARA, busca o registro APLIC
    com mesmo CNPJ e menor delta de data (máx. LIMIAR_DIAS_T2 dias).

    Retorna (matched, remanescentes, new_matched_aplic_indices).
    """
    if df_pncp_orfaos.empty:
        return pd.DataFrame(), df_pncp_orfaos.copy(), set()

    cnpjs_aplic   = set(df_aplic['_cnpj_mapeado'].dropna().unique())
    pncp_com_cnpj = df_pncp_orfaos[df_pncp_orfaos['orgaoEntidade_cnpj'].isin(cnpjs_aplic)]

    if pncp_com_cnpj.empty:
        logger.info("[Tier 2] Nenhum CNPJ PNCP presente no DE-PARA.")
        return pd.DataFrame(), df_pncp_orfaos.copy(), set()

    col_data_aplic = next((c for c in df_aplic.columns if 'abertura' in c.lower()), None)

    matched_rows: list[dict] = []
    matched_pncp_idx: set = set()
    matched_aplic_idx: set = set()

    for pncp_ri, pncp_row in pncp_com_cnpj.iterrows():
        cnpj = pncp_row['orgaoEntidade_cnpj']
        candidatos_aplic = df_aplic[
            (df_aplic['_cnpj_mapeado'] == cnpj) &
            (~df_aplic.index.isin(already_matched_aplic | matched_aplic_idx))
        ]
        if candidatos_aplic.empty:
            continue

        data_pncp = pd.to_datetime(pncp_row.get('dataPublicacaoPncp'), errors='coerce')
        if pd.notna(data_pncp) and getattr(data_pncp, 'tzinfo', None):
            data_pncp = data_pncp.tz_localize(None)

        melhor_delta    = float('inf')
        melhor_aplic_ri = None

        for aplic_ri, aplic_row in candidatos_aplic.iterrows():
            if col_data_aplic and pd.notna(data_pncp):
                data_a = pd.to_datetime(aplic_row.get(col_data_aplic), errors='coerce')
                if pd.notna(data_a):
                    delta = abs((data_pncp - data_a).days)
                    if delta <= LIMIAR_DIAS_T2 and delta < melhor_delta:
                        melhor_delta    = delta
                        melhor_aplic_ri = aplic_ri
            else:
                # Sem data disponível: aceita qualquer APLIC com mesmo CNPJ
                melhor_aplic_ri = aplic_ri
                melhor_delta    = 0
                break

        if melhor_aplic_ri is not None:
            row = pncp_row.to_dict()
            for col in df_aplic.columns:
                if col not in row:
                    row[col] = df_aplic.loc[melhor_aplic_ri, col]
            row['_origem_merge']      = 'secundario_cnpj_data'
            row['_delta_dias_tier2']  = melhor_delta
            matched_rows.append(row)
            matched_pncp_idx.add(pncp_ri)
            matched_aplic_idx.add(melhor_aplic_ri)

    if not matched_rows:
        logger.info(f"[Tier 2] 0 matches | {len(df_pncp_orfaos)} remanescentes PNCP")
        return pd.DataFrame(), df_pncp_orfaos.copy(), set()

    matched      = pd.DataFrame(matched_rows)
    remanescentes = df_pncp_orfaos[~df_pncp_orfaos.index.isin(matched_pncp_idx)].copy()
    logger.info(f"[Tier 2] {len(matched)} matches | {len(remanescentes)} remanescentes PNCP")
    return matched, remanescentes, matched_aplic_idx


def _merge_terciario(
    df_pncp_rem: pd.DataFrame,
    df_aplic: pd.DataFrame,
    already_matched_aplic: set,
) -> tuple[pd.DataFrame, set]:
    """
    Tier 3 — Fallback estrutural: município + número + ano + modalidade.
    Último recurso para registros não resolvidos pelos tiers semântico e de CNPJ.

    Retorna (result_df, new_matched_aplic_indices).
    """
    if df_pncp_rem.empty:
        return pd.DataFrame(), set()

    df_aplic_disp = df_aplic[~df_aplic.index.isin(already_matched_aplic)].copy()
    df_aplic_disp['_aplic_orig_idx'] = df_aplic_disp.index

    merged = df_pncp_rem.merge(
        df_aplic_disp,
        left_on=['_municipio_norm', '_numero_puro', '_ano_extraido', 'modalidadeId'],
        right_on=['_municipio_norm', '_numero_puro', '_ano_extraido', '_modalidade_pncp_id'],
        how='left',
        suffixes=('', '_aplic')
    )
    merged['_origem_merge'] = merged['_cnpj_mapeado'].apply(
        lambda x: 'terciario_estrutural' if pd.notna(x) else 'sem_match'
    )

    new_matched = set(merged['_aplic_orig_idx'].dropna().astype(int))
    merged = merged.drop(columns=['_aplic_orig_idx'], errors='ignore')

    n_matched = (merged['_origem_merge'] == 'terciario_estrutural').sum()
    n_sem     = (merged['_origem_merge'] == 'sem_match').sum()
    logger.info(f"[Tier 3] {n_matched} matches estruturais | {n_sem} sem match")
    return merged, new_matched


# ---------------------------------------------------------------------------
# Enriquecimento pós-merge
# ---------------------------------------------------------------------------

def _calcular_fuzzy_score(row: pd.Series) -> float:
    """Calcula fuzzy score entre objeto PNCP e objetivo APLIC."""
    try:
        from rapidfuzz import fuzz as rffuzz
    except ImportError:
        return 0.0

    obj  = row.get('_objeto_norm', '')
    alvo = row.get('_objetivo_norm', '')
    if not obj or not alvo or not isinstance(obj, str) or not isinstance(alvo, str):
        return 0.0
    return float(rffuzz.token_sort_ratio(obj, alvo))


def _calcular_score_composto(row: pd.Series) -> float:
    """
    Score composto pós-match:
      50% fuzzy texto (objeto PNCP vs objetivo APLIC)
      30% proximidade de valor (delta_percentual)
      20% proximidade de data  (delta_dias)

    Componentes ausentes recebem score neutro (50) para não penalizar
    registros com dados incompletos.
    """
    # Texto
    score_texto = float(row.get('fuzzy_score') or 0)

    # Valor
    delta_val = row.get('delta_percentual')
    if delta_val is None or (isinstance(delta_val, float) and pd.isna(delta_val)):
        score_valor = 50.0
    elif delta_val <= 10:
        score_valor = 100.0
    elif delta_val <= 30:
        score_valor = 50.0
    else:
        score_valor = 0.0

    # Data
    delta_dias = row.get('delta_dias')
    if delta_dias is None or (isinstance(delta_dias, float) and pd.isna(delta_dias)):
        score_data = 50.0
    elif delta_dias <= 15:
        score_data = 100.0
    elif delta_dias <= 30:
        score_data = 50.0
    else:
        score_data = 0.0

    return round(score_texto * 0.5 + score_valor * 0.3 + score_data * 0.2, 2)


def calcular_delta_financeiro(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula delta percentual entre valor estimado PNCP e APLIC."""
    df = df.copy()

    val_pncp = pd.to_numeric(df.get('valorTotalEstimado', 0), errors='coerce').fillna(0.0)

    if '_valor_estimado_float' not in df.columns:
        df['delta_percentual'] = None
        df['validacao_financeira'] = 'SEM_VALOR'
        return df

    val_aplic = pd.to_numeric(df['_valor_estimado_float'], errors='coerce').fillna(0.0)

    sem_valor = (val_pncp == 0) | (val_aplic == 0)
    denominador = val_pncp.where(val_pncp > 0, val_aplic).where(lambda x: x > 0, 1.0)
    delta = ((val_pncp - val_aplic).abs() / denominador * 100).round(2)

    df['delta_percentual'] = delta.where(~sem_valor, None)
    df['validacao_financeira'] = 'OK'
    df.loc[sem_valor, 'validacao_financeira'] = 'SEM_VALOR'
    df.loc[(~sem_valor) & (delta > 10), 'validacao_financeira'] = 'DIVERGENTE'

    return df


def calcular_delta_temporal(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula a diferença em dias entre a publicação PNCP e a Abertura do APLIC."""
    df = df.copy()
    col_data_aplic = next((c for c in df.columns if 'abertura' in c.lower()), None)
    col_data_pncp = 'dataPublicacaoPncp' if 'dataPublicacaoPncp' in df.columns else None
    
    if col_data_aplic and col_data_pncp:
        data_p = pd.to_datetime(df[col_data_pncp], errors='coerce').dt.tz_localize(None)
        data_a = pd.to_datetime(df[col_data_aplic], errors='coerce').dt.tz_localize(None)
        df['delta_dias'] = (data_p - data_a).dt.days.abs()
    else:
        df['delta_dias'] = None
    return df


def classificar_status(df: pd.DataFrame) -> pd.DataFrame:
    """
    Classifica status usando score composto (50% texto + 30% valor + 20% data).
    Matches com score < LIMIAR_MATCH_PARCIAL são rebaixados para SEM_MATCH.
    """
    df = df.copy()

    sem_match_mask = df['_origem_merge'] == 'sem_match'
    df['status_cruzamento'] = 'SEM_MATCH'

    if 'estrategia_match' not in df.columns:
        df['estrategia_match'] = df['_origem_merge']
    else:
        df['estrategia_match'] = df['estrategia_match'].fillna(df['_origem_merge'])

    # Score composto apenas para linhas com match
    df['score_composto'] = 0.0
    mask_com_match = ~sem_match_mask
    if mask_com_match.any():
        df.loc[mask_com_match, 'score_composto'] = (
            df[mask_com_match].apply(_calcular_score_composto, axis=1)
        )

    confirmado = mask_com_match & (df['score_composto'] >= LIMIAR_MATCH_CONFIRMADO)
    parcial    = mask_com_match & (df['score_composto'] >= LIMIAR_MATCH_PARCIAL) & (~confirmado)
    # score < LIMIAR_MATCH_PARCIAL → permanece SEM_MATCH (rebaixado)

    df.loc[confirmado, 'status_cruzamento'] = 'MATCH_CONFIRMADO'
    df.loc[parcial,    'status_cruzamento'] = 'MATCH_PARCIAL'

    # Respeita override APENAS_APLIC
    df.loc[df['estrategia_match'] == 'sem_par_pncp', 'status_cruzamento'] = 'APENAS_APLIC'

    return df


def selecionar_colunas_saida(df: pd.DataFrame) -> pd.DataFrame:
    """Remove colunas auxiliares com prefixo _."""
    colunas_aux = [c for c in df.columns if c.startswith('_')]
    return df.drop(columns=colunas_aux, errors='ignore')


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------


def _aplic_sem_pncp(
    df_a: pd.DataFrame,
    matched_aplic_indices: set,
) -> pd.DataFrame:
    """
    Identifica registros APLIC sem par em nenhum tier PNCP.
    Usa o conjunto de índices reais do DataFrame APLIC acumulados pelos tiers.
    Retorna DataFrame com status_cruzamento = 'APENAS_APLIC'.
    """
    nao_matched = df_a[~df_a.index.isin(matched_aplic_indices)].copy()

    if nao_matched.empty:
        return pd.DataFrame()

    nao_matched['status_cruzamento']    = 'APENAS_APLIC'
    nao_matched['estrategia_match']     = 'sem_par_pncp'
    nao_matched['fuzzy_score']          = 0.0
    nao_matched['score_composto']       = 0.0
    nao_matched['delta_percentual']     = None
    nao_matched['validacao_financeira'] = 'SEM_VALOR'

    logger.info(f"[APLIC sem PNCP] {len(nao_matched)} registros APLIC sem par no PNCP")
    return nao_matched


def crossmatch(df_pncp: pd.DataFrame, df_aplic: pd.DataFrame) -> pd.DataFrame:
    """
    Cruzamento PNCP × APLIC em cascata (3 tiers) + lado APLIC sem par.

    Tier 1 — Semântico + Financeiro (mais discriminante):
        fuzzy objeto/objetivo + delta valor
    Tier 2 — CNPJ + Data (confirmação estrutural):
        CNPJ exato + proximidade de data
    Tier 3 — Estrutural (fallback):
        município + número + ano + modalidade

    Colunas de diagnóstico no resultado:
        status_cruzamento, estrategia_match, fuzzy_score, score_composto,
        delta_percentual, validacao_financeira, delta_dias
    """
    logger.info("=" * 60)
    logger.info("INICIANDO CROSSMATCH PNCP × APLIC")
    logger.info(f"PNCP: {len(df_pncp)} registros | APLIC: {len(df_aplic)} registros")

    # 1. Preparação
    df_p = preparar_pncp(df_pncp)
    df_a = preparar_aplic(df_aplic)

    # 2. Deduplicação
    df_p = deduplicar_pncp(df_p)
    df_a = deduplicar_aplic(df_a)

    # 3. Tier 1 — Semântico + Financeiro
    matched_t1, pncp_orfaos, matched_aplic_t1 = _merge_primario(df_p, df_a)

    # 4. Tier 2 — CNPJ + Data
    matched_t2, remanescentes, matched_aplic_t2 = _merge_secundario(
        pncp_orfaos, df_a, matched_aplic_t1
    )

    # 5. Tier 3 — Estrutural (fallback)
    all_matched_aplic = matched_aplic_t1 | matched_aplic_t2
    matched_t3, matched_aplic_t3 = _merge_terciario(remanescentes, df_a, all_matched_aplic)
    all_matched_aplic |= matched_aplic_t3

    # 6. Consolida lado PNCP
    partes = [p for p in [matched_t1, matched_t2, matched_t3]
              if isinstance(p, pd.DataFrame) and not p.empty]
    if not partes:
        logger.warning("Nenhum resultado após cruzamento.")
        df_pncp_final = pd.DataFrame()
    else:
        df_pncp_final = pd.concat(partes, ignore_index=True)

    # 7. Enriquecimento + classificação
    if not df_pncp_final.empty:
        df_pncp_final['fuzzy_score'] = df_pncp_final.apply(_calcular_fuzzy_score, axis=1)
        df_pncp_final = calcular_delta_financeiro(df_pncp_final)
        df_pncp_final = calcular_delta_temporal(df_pncp_final)
        df_pncp_final = classificar_status(df_pncp_final)

    # 8. Lado APLIC: registros sem par em nenhum tier
    df_apenas_aplic = _aplic_sem_pncp(df_a, all_matched_aplic)

    # 9. Junta os dois lados
    partes_final = [p for p in [df_pncp_final, df_apenas_aplic]
                    if isinstance(p, pd.DataFrame) and not p.empty]
    df_final = pd.concat(partes_final, ignore_index=True) if partes_final else pd.DataFrame()

    # 10. Remove colunas auxiliares
    df_final = selecionar_colunas_saida(df_final)

    # 11. Log de contagens
    for col in ('status_cruzamento', 'estrategia_match'):
        if col in df_final.columns:
            logger.info(f"Contagem por {col}:")
            for val, count in df_final[col].value_counts().items():
                logger.info(f"  {val}: {count}")

    logger.info(f"CROSSMATCH CONCLUÍDO: {len(df_final)} registros no resultado final")
    logger.info("=" * 60)
    return df_final


def carregar_aplic(caminho: Path) -> pd.DataFrame:
    """
    Carrega o extrato APLIC a partir de CSV.
    Corrige problemas de vírgula decimal que quebram colunas extras.
    """
    logger.info(f"Carregando APLIC de: {caminho}")

    with open(caminho, 'r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        header = next(reader)
        expected_len = len(header)

        fixed_rows = []
        for row in reader:
            if not any(row):
                continue

            # Mescla colunas extras causadas por vírgula decimal no CSV
            while len(row) > expected_len:
                merged_col = False
                for i in range(len(row) - 1):
                    if re.match(r'^\d+$', row[i]) and re.match(r'^\d{1,2}$', row[i + 1]):
                        row[i] = f"{row[i]}.{row[i + 1]}"
                        del row[i + 1]
                        merged_col = True
                        break
                if not merged_col:
                    break

            while len(row) < expected_len:
                row.append("")

            fixed_rows.append(row[:expected_len])

    df = pd.DataFrame(fixed_rows, columns=header)
    logger.info(f"APLIC carregado: {df.shape[0]} linhas × {df.shape[1]} colunas")
    return df


# ---------------------------------------------------------------------------
# CLI standalone
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    if len(sys.argv) < 3:
        print("Uso: python crossmatch.py <pncp.xlsx> <aplic.csv>")
        sys.exit(1)

    pncp_path = Path(sys.argv[1])
    aplic_path = Path(sys.argv[2])

    if not pncp_path.exists():
        print(f"Arquivo PNCP não encontrado: {pncp_path}")
        sys.exit(1)
    if not aplic_path.exists():
        print(f"Arquivo APLIC não encontrado: {aplic_path}")
        sys.exit(1)

    df_pncp = pd.read_excel(pncp_path, dtype=str)
    df_aplic = carregar_aplic(aplic_path)

    df_resultado = crossmatch(df_pncp, df_aplic)

    if df_resultado.empty:
        print("Resultado vazio. Verifique os arquivos de entrada.")
        sys.exit(0)

    saida = pncp_path.parent / f"crossmatch_{pncp_path.stem}.xlsx"
    df_resultado.to_excel(saida, index=False)
    print(f"\nResultado exportado para: {saida}")
    print(f"Total de registros: {len(df_resultado)}")
    if 'status_cruzamento' in df_resultado.columns:
        print("\nStatus do cruzamento:")
        print(df_resultado['status_cruzamento'].value_counts().to_string())
    if 'estrategia_match' in df_resultado.columns:
        print("\nEstratégia de match:")
        print(df_resultado['estrategia_match'].value_counts().to_string())

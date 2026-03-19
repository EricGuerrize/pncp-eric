"""
crossmatch.py — Cruzamento PNCP × APLIC (TCE-MT)

Cruza o DataFrame de contratações do PNCP com o extrato Oracle do APLIC,
validando licitações municipais/estaduais para fins de auditoria.

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
    # Adicionar mais mapeamentos conforme necessário
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
LIMIAR_MATCH_PARCIAL = 70


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
    Normaliza texto: lowercase + remove acentos (NFKD) + colapsa espaços + strip.
    """
    if not isinstance(texto, str):
        return ""
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(c for c in texto if not unicodedata.combining(c))
    texto = texto.lower()
    texto = re.sub(r'\s+', ' ', texto).strip()
    return texto


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
    df['_tem_homologado'] = (df.get('valorTotalHomologado', 0) > 0).astype(int)
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

def _merge_primario(df_pncp: pd.DataFrame, df_aplic: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Tier 1: merge forte por CNPJ + número + ano + modalidade.
    Retorna (matched, pncp_orfaos).
    """
    cnpjs_aplic = set(df_aplic['_cnpj_mapeado'].dropna().unique())
    pncp_com_cnpj = df_pncp[df_pncp['orgaoEntidade_cnpj'].isin(cnpjs_aplic)].copy()
    pncp_sem_cnpj = df_pncp[~df_pncp['orgaoEntidade_cnpj'].isin(cnpjs_aplic)].copy()

    if pncp_com_cnpj.empty:
        logger.info("[Tier 1] Nenhum CNPJ PNCP presente no DE-PARA. Todos vão para Tier 2.")
        return pd.DataFrame(), df_pncp

    # Preserva o índice original para identificar quais linhas matcharam
    pncp_com_cnpj = pncp_com_cnpj.reset_index(drop=False).rename(columns={'index': '_idx_orig'})

    merged = pncp_com_cnpj.merge(
        df_aplic,
        left_on=['orgaoEntidade_cnpj', '_numero_puro', '_ano_extraido', 'modalidadeId'],
        right_on=['_cnpj_mapeado', '_numero_puro', '_ano_extraido', '_modalidade_pncp_id'],
        how='left',
        suffixes=('', '_aplic')
    )
    merged['_origem_merge'] = merged['_cnpj_mapeado'].apply(
        lambda x: 'primario' if pd.notna(x) else 'sem_match'
    )

    matched = merged[merged['_cnpj_mapeado'].notna()].copy()

    # Índices originais que matcharam no Tier 1
    idx_matchados = set(matched['_idx_orig'].dropna().astype(int))

    # Órfãos: sem CNPJ no DE-PARA + com CNPJ mas sem match no APLIC
    pncp_sem_match_t1 = df_pncp[
        df_pncp.orgaoEntidade_cnpj.isin(cnpjs_aplic) &
        ~df_pncp.index.isin(idx_matchados)
    ]
    pncp_orfaos = pd.concat([pncp_sem_cnpj, pncp_sem_match_t1], ignore_index=True)

    # Remove coluna auxiliar de índice
    matched = matched.drop(columns=['_idx_orig'], errors='ignore')

    logger.info(f"[Tier 1] {len(matched)} matches | {len(pncp_orfaos)} órfãos PNCP")
    return matched, pncp_orfaos


def _merge_secundario(df_pncp_orfaos: pd.DataFrame, df_aplic: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Tier 2: fallback por município + número + ano + modalidade.
    Retorna (matched, remanescentes_pncp).
    """
    if df_pncp_orfaos.empty:
        return pd.DataFrame(), df_pncp_orfaos

    merged = df_pncp_orfaos.merge(
        df_aplic,
        left_on=['_municipio_norm', '_numero_puro', '_ano_extraido', 'modalidadeId'],
        right_on=['_municipio_norm', '_numero_puro', '_ano_extraido', '_modalidade_pncp_id'],
        how='left',
        suffixes=('', '_aplic')
    )
    merged['_origem_merge'] = merged['_cnpj_mapeado'].apply(
        lambda x: 'secundario_municipio' if pd.notna(x) else 'sem_match'
    )

    matched = merged[merged['_cnpj_mapeado'].notna()].copy()
    matched_orig_idx = set(matched.index)
    remanescentes = df_pncp_orfaos[~df_pncp_orfaos.index.isin(matched_orig_idx)].copy()

    logger.info(f"[Tier 2] {len(matched)} matches | {len(remanescentes)} remanescentes PNCP")
    return matched, remanescentes


def _merge_terciario(df_pncp_rem: pd.DataFrame, df_aplic: pd.DataFrame) -> pd.DataFrame:
    """
    Tier 3: fuzzy match por objeto/objetivo dentro do mesmo (ano, município).
    """
    if df_pncp_rem.empty:
        return pd.DataFrame()

    try:
        from rapidfuzz import process as rfprocess, fuzz as rffuzz
    except ImportError:
        logger.warning("rapidfuzz não instalado. Tier 3 (fuzzy) ignorado.")
        df_out = df_pncp_rem.copy()
        df_out['_origem_merge'] = 'sem_match'
        return df_out

    resultados = []
    grupos = df_pncp_rem.groupby(['_ano_extraido', '_municipio_norm'], dropna=False)

    for (ano, mun), grupo_pncp in grupos:
        grupo_aplic = df_aplic[
            (df_aplic['_ano_extraido'] == ano) &
            (df_aplic['_municipio_norm'] == mun)
        ]

        if grupo_aplic.empty:
            for _, row in grupo_pncp.iterrows():
                d = row.to_dict()
                d['_origem_merge'] = 'sem_match'
                resultados.append(d)
            continue

        objetos_pncp = grupo_pncp['_objeto_norm'].fillna('').tolist()
        objetivos_aplic = grupo_aplic['_objetivo_norm'].fillna('').tolist()

        matrix = rfprocess.cdist(
            objetos_pncp, objetivos_aplic,
            scorer=rffuzz.token_sort_ratio
        )

        for i, (_, row_pncp) in enumerate(grupo_pncp.iterrows()):
            scores = matrix[i]
            melhor_score = float(scores.max()) if len(scores) > 0 else 0.0
            melhor_idx = int(scores.argmax()) if melhor_score >= LIMIAR_MATCH_PARCIAL else None

            d = row_pncp.to_dict()
            if melhor_idx is not None:
                aplic_row = grupo_aplic.iloc[melhor_idx]
                for col in aplic_row.index:
                    if col not in d:
                        d[col] = aplic_row[col]
                d['_origem_merge'] = 'terciario_fuzzy'
                d['_fuzzy_score_tier3'] = melhor_score
            else:
                d['_origem_merge'] = 'sem_match'
            resultados.append(d)

    if not resultados:
        return pd.DataFrame()

    resultado = pd.DataFrame(resultados)
    n_fuzzy = (resultado['_origem_merge'] == 'terciario_fuzzy').sum()
    n_sem = (resultado['_origem_merge'] == 'sem_match').sum()
    logger.info(f"[Tier 3] {n_fuzzy} matches fuzzy | {n_sem} sem match")
    return resultado


# ---------------------------------------------------------------------------
# Enriquecimento pós-merge
# ---------------------------------------------------------------------------

def _calcular_fuzzy_score(row: pd.Series) -> float:
    """Calcula fuzzy score entre objeto PNCP e objetivo APLIC."""
    try:
        from rapidfuzz import fuzz as rffuzz
    except ImportError:
        return 0.0

    obj = row.get('_objeto_norm', '')
    alvo = row.get('_objetivo_norm', '')
    if not obj or not alvo or not isinstance(obj, str) or not isinstance(alvo, str):
        return 0.0
    return float(rffuzz.token_sort_ratio(obj, alvo))


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


def classificar_status(df: pd.DataFrame) -> pd.DataFrame:
    """Classifica status do cruzamento baseado no fuzzy_score e origem do merge."""
    df = df.copy()

    sem_match_mask = df['_origem_merge'] == 'sem_match'

    df['status_cruzamento'] = 'SEM_MATCH'
    df.loc[~sem_match_mask & (df['fuzzy_score'] >= LIMIAR_MATCH_CONFIRMADO), 'status_cruzamento'] = 'MATCH_CONFIRMADO'
    df.loc[
        ~sem_match_mask &
        (df['fuzzy_score'] >= LIMIAR_MATCH_PARCIAL) &
        (df['fuzzy_score'] < LIMIAR_MATCH_CONFIRMADO),
        'status_cruzamento'
    ] = 'MATCH_PARCIAL'

    df['estrategia_match'] = df['_origem_merge']
    return df


def selecionar_colunas_saida(df: pd.DataFrame) -> pd.DataFrame:
    """Remove colunas auxiliares com prefixo _."""
    colunas_aux = [c for c in df.columns if c.startswith('_')]
    return df.drop(columns=colunas_aux, errors='ignore')


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------

def _make_chave(df: pd.DataFrame, col_num: str, col_ano: str, col_mod: str) -> pd.Series:
    """Gera chave string normalizada (inteiros) para comparação entre DataFrames."""
    def to_int_str(s):
        return pd.to_numeric(s, errors='coerce').fillna(-1).astype(int).astype(str)
    return to_int_str(df[col_num]) + '_' + to_int_str(df[col_ano]) + '_' + to_int_str(df[col_mod])


def _aplic_sem_pncp(df_a: pd.DataFrame, df_pncp_matched: pd.DataFrame) -> pd.DataFrame:
    """
    Identifica registros do APLIC que não encontraram nenhum par no PNCP.
    Retorna DataFrame com status_cruzamento = 'APENAS_APLIC'.
    """
    # Chave do APLIC: número + ano + modalidade
    chave_aplic = _make_chave(df_a, '_numero_puro', '_ano_extraido', '_modalidade_pncp_id')

    # Chave dos matches PNCP: apenas as linhas que efetivamente matcharam
    if not df_pncp_matched.empty and '_origem_merge' in df_pncp_matched.columns:
        df_com_match = df_pncp_matched[
            df_pncp_matched['_origem_merge'].isin(['primario', 'secundario_municipio', 'terciario_fuzzy'])
        ]
    else:
        df_com_match = pd.DataFrame()

    if not df_com_match.empty and '_modalidade_pncp_id' in df_com_match.columns:
        keys_matched = set(
            _make_chave(df_com_match, '_numero_puro', '_ano_extraido', '_modalidade_pncp_id')
        )
    else:
        keys_matched = set()

    aplic_nao_matched = df_a[~chave_aplic.isin(keys_matched)].copy()

    if aplic_nao_matched.empty:
        return pd.DataFrame()

    aplic_nao_matched['status_cruzamento'] = 'APENAS_APLIC'
    aplic_nao_matched['estrategia_match'] = 'sem_par_pncp'
    aplic_nao_matched['fuzzy_score'] = 0.0
    aplic_nao_matched['delta_percentual'] = None
    aplic_nao_matched['validacao_financeira'] = 'SEM_VALOR'

    logger.info(f"[APLIC sem PNCP] {len(aplic_nao_matched)} registros APLIC sem par no PNCP")
    return aplic_nao_matched


def crossmatch(df_pncp: pd.DataFrame, df_aplic: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza o cruzamento PNCP × APLIC em cascata (3 tiers) + lado APLIC sem par.

    O resultado cobre os 3 cenários de auditoria:
      - MATCH_CONFIRMADO / MATCH_PARCIAL: licitação presente nos dois sistemas
      - SEM_MATCH: publicada no PNCP mas sem registro no APLIC
      - APENAS_APLIC: registrada no APLIC mas sem publicação no PNCP

    Returns:
        DataFrame com colunas originais PNCP + colunas APLIC +
        colunas de diagnóstico: status_cruzamento, fuzzy_score,
        delta_percentual, validacao_financeira, estrategia_match.
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

    # 3. Tier 1 — primário (CNPJ)
    matched_t1, pncp_orfaos = _merge_primario(df_p, df_a)

    # 4. Tier 2 — secundário (município)
    matched_t2, remanescentes = _merge_secundario(pncp_orfaos, df_a)

    # 5. Tier 3 — terciário (fuzzy)
    matched_t3 = _merge_terciario(remanescentes, df_a)

    # 6. Consolida lado PNCP
    partes = [p for p in [matched_t1, matched_t2, matched_t3]
              if isinstance(p, pd.DataFrame) and not p.empty]
    if not partes:
        logger.warning("Nenhum resultado após cruzamento.")
        df_pncp_final = pd.DataFrame()
    else:
        df_pncp_final = pd.concat(partes, ignore_index=True)

    # 7. Enriquecimento do lado PNCP
    if not df_pncp_final.empty:
        df_pncp_final['fuzzy_score'] = df_pncp_final.apply(_calcular_fuzzy_score, axis=1)
        df_pncp_final = calcular_delta_financeiro(df_pncp_final)
        df_pncp_final = classificar_status(df_pncp_final)

    # 8. Lado APLIC: registros sem par no PNCP (APENAS_APLIC)
    df_apenas_aplic = _aplic_sem_pncp(df_a, df_pncp_final)

    # 9. Junta os dois lados
    partes_final = [p for p in [df_pncp_final, df_apenas_aplic]
                    if isinstance(p, pd.DataFrame) and not p.empty]
    df_final = pd.concat(partes_final, ignore_index=True) if partes_final else pd.DataFrame()

    # 10. Remove colunas auxiliares
    df_final = selecionar_colunas_saida(df_final)

    # 11. Log de contagens
    logger.info("Contagem por status_cruzamento:")
    for status, count in df_final['status_cruzamento'].value_counts().items():
        logger.info(f"  {status}: {count}")
    logger.info("Contagem por estrategia_match:")
    for estrategia, count in df_final['estrategia_match'].value_counts().items():
        logger.info(f"  {estrategia}: {count}")

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

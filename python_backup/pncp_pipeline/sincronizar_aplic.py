"""
sincronizar_aplic.py — Extrai APLIC do Oracle e sobe direto para o Firebase.

Rode este script na máquina do TCE (que tem acesso ao Oracle).
Não precisa de GitHub Actions, Supabase ou nada extra.

Uso:
    python sincronizar_aplic.py --cidades rondolandia acorizal jangada "lucas do rio verde" sinop --ano 2026
    python sincronizar_aplic.py --cidades sinop --ano 2025 2026
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
import unicodedata
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, str(Path(__file__).parent))

from aplic_extractor import descobrir_ugs, enriquecer_cnpj_do_pncp, atualizar_orgaos_json, extrair_aplic
from crossmatch import preparar_aplic, load_orgaos
from firebase_sync import _inicializar_firebase, _sub, _adicionar_dias_uteis, _fval, _dt

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

SUB_APLIC_RAW = "aplic_raw"


# ---------------------------------------------------------------------------
# Slug
# ---------------------------------------------------------------------------

def _slug(nome: str) -> str:
    nfkd = unicodedata.normalize("NFKD", str(nome))
    ascii_ = nfkd.encode("ASCII", "ignore").decode("ASCII")
    return ascii_.lower().strip().replace(" ", "_")


# ---------------------------------------------------------------------------
# Upload APLIC → Firebase
# ---------------------------------------------------------------------------

def _doc_aplic_raw(row: pd.Series, municipio_slug: str) -> dict:
    from google.cloud.firestore_v1 import SERVER_TIMESTAMP
    return {
        "municipio":  municipio_slug,
        "orgao":      str(row.get("_orgao_nome") or row.get("UG") or "")[:80],
        "modalidade": str(row.get("Modalidade") or "")[:60],
        "mod_id":     str(row.get("_mod_id_raw") or ""),
        "numero":     str(row.get("_numero_puro") or row.get("Nº Licitação") or ""),
        "ano":        str(row.get("_ano_extraido") or row.get("Exercício") or ""),
        "objeto":     str(row.get("_objetivo_norm") or row.get("Objetivo") or row.get("Motivo") or "")[:300],
        "valor":      _fval(row.get("Valor Estimado")),
        "cnpj":       str(row.get("_cnpj_mapeado") or ""),
        "dataAPLIC":  _dt(row.get("Data Abertura") or ""),
        "atualizadoEm": SERVER_TIMESTAMP,
    }


def _doc_id_aplic(row: pd.Series) -> str:
    cnpj   = re.sub(r"\D", "", str(row.get("cnpj") or "")).replace("/", "_")
    numero = str(row.get("numero") or "").replace("/", "_")
    ano    = str(row.get("ano") or "")
    mod    = str(row.get("mod_id") or "00")
    return f"{cnpj}-{numero}-{ano}-{mod}" if cnpj else ""


def upload_aplic_firebase(df_aplic_prep: pd.DataFrame, municipio_slug: str) -> int:
    """Grava registros APLIC em municipios/{slug}/aplic_raw no Firestore."""
    from google.cloud.firestore_v1 import WriteBatch

    db  = _inicializar_firebase()
    col = _sub(db, municipio_slug, SUB_APLIC_RAW)

    total    = 0
    batch    = db.batch()
    em_lote  = 0

    for _, row in df_aplic_prep.iterrows():
        doc = _doc_aplic_raw(row, municipio_slug)
        doc_id = _doc_id_aplic(doc)
        if not doc_id:
            continue

        ref = col.document(doc_id)
        batch.set(ref, doc, merge=True)
        em_lote += 1
        total   += 1

        if em_lote >= 400:
            batch.commit()
            batch   = db.batch()
            em_lote = 0

    if em_lote:
        batch.commit()

    logger.info(f"[Firebase] {municipio_slug}: {total} registros APLIC gravados em aplic_raw")
    return total


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def run(cidades: list[str], anos: list[int]) -> None:
    # 1. Descobre UGs no Oracle (uma vez para todas as cidades)
    logger.info(f"Descobrindo UGs para: {cidades}")
    df_ugs = descobrir_ugs(cidades)
    if df_ugs.empty:
        logger.error("Nenhum UG encontrado. Verifique nomes dos municípios e conexão Oracle.")
        sys.exit(1)

    df_ugs = enriquecer_cnpj_do_pncp(df_ugs)
    atualizar_orgaos_json(df_ugs)
    load_orgaos()

    ugs = df_ugs["ug_code"].tolist()

    for ano in anos:
        logger.info(f"=== Extraindo APLIC {ano} ===")
        df_raw = extrair_aplic(ugs, ano)
        if df_raw.empty:
            logger.warning(f"Extração APLIC {ano} retornou vazia.")
            continue

        # Prepara (normaliza campos, mapeia UGs, etc.) — mesma lógica do crossmatch
        df_prep = preparar_aplic(df_raw)
        if df_prep.empty:
            logger.warning(f"Nenhum registro válido após preparar APLIC {ano}.")
            continue

        # Agrupa por município e sobe para o Firebase
        mun_col = next(
            (c for c in df_prep.columns if c in ("municipio", "Município", "MUNICIPIO")),
            None
        )

        if mun_col:
            for municipio, grupo in df_prep.groupby(mun_col):
                slug = _slug(str(municipio))
                upload_aplic_firebase(grupo.reset_index(drop=True), slug)
        else:
            # Fallback: sobe para cada cidade solicitada
            for cidade in cidades:
                slug = _slug(cidade)
                upload_aplic_firebase(df_prep.reset_index(drop=True), slug)

    logger.info("Sincronização APLIC → Firebase concluída.")


def main():
    parser = argparse.ArgumentParser(
        description="Extrai APLIC do Oracle e sincroniza com Firebase."
    )
    parser.add_argument(
        "--cidades", nargs="+",
        default=["rondolandia", "acorizal", "jangada", "lucas do rio verde", "sinop"],
        help="Nomes dos municípios (ex: sinop rondolandia acorizal jangada 'lucas do rio verde')",
    )
    parser.add_argument(
        "--ano", nargs="+", type=int, default=[2026],
        help="Ano(s) a extrair. Ex: --ano 2025 2026",
    )
    args = parser.parse_args()
    run(args.cidades, args.ano)


if __name__ == "__main__":
    main()

"""
backfill_firebase.py — Popula o Firestore com todos os xlsx existentes em output/.

Uso:
    python backfill_firebase.py          # lê todos os pncp_contratacoes_MT_*.xlsx
    python backfill_firebase.py --start 20260201 --end 20260325  # filtra por data
"""

import argparse
import logging
import re
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).parent / "output"


def _xlsx_para_processar(start: str = None, end: str = None):
    """Retorna lista de xlsx de contratações ordenados, opcionalmente filtrados por data."""
    arquivos = sorted(OUTPUT_DIR.glob("pncp_contratacoes_MT_????????.xlsx"))
    if start:
        arquivos = [f for f in arquivos if f.stem.split("_")[-1] >= start]
    if end:
        arquivos = [f for f in arquivos if f.stem.split("_")[-1] <= end]
    return arquivos


def _carregar(xlsx_path: Path) -> pd.DataFrame | None:
    try:
        df = pd.read_excel(xlsx_path, dtype=str)
        df["orgaoEntidade_cnpj"] = df["orgaoEntidade_cnpj"].astype(str).apply(
            lambda x: re.sub(r"\D", "", x)
        )
        return df
    except Exception as e:
        logger.warning(f"  Erro ao ler {xlsx_path.name}: {e}")
        return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill Firebase com histórico PNCP")
    parser.add_argument("--start", metavar="YYYYMMDD", help="Data inicial (filtro)")
    parser.add_argument("--end",   metavar="YYYYMMDD", help="Data final (filtro)")
    args = parser.parse_args()

    from firebase_sync import sincronizar

    arquivos = _xlsx_para_processar(args.start, args.end)
    logger.info(f"Backfill: {len(arquivos)} arquivo(s) encontrado(s) em output/")

    total = {"inseridos": 0, "atualizados": 0, "alertas": 0, "municipios_processados": 0}

    for i, xlsx in enumerate(arquivos, 1):
        logger.info(f"[{i}/{len(arquivos)}] {xlsx.name}")
        df = _carregar(xlsx)
        if df is None:
            continue
        r = sincronizar(df)
        total["inseridos"]              += r["inseridos"]
        total["atualizados"]            += r["atualizados"]
        total["alertas"]                += r["alertas"]
        total["municipios_processados"] += r.get("municipios_processados", 0)

    print(f"\n=== Backfill concluído ===")
    print(f"  Arquivos processados: {len(arquivos)}")
    print(f"  Municípios únicos*:   {total['municipios_processados']} (soma por arquivo)")
    print(f"  Inseridos:            {total['inseridos']}")
    print(f"  Atualizados:          {total['atualizados']}")
    print(f"  Alertas ativados:     {total['alertas']}")

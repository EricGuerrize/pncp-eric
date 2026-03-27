"""
backfill_firebase.py — Popula o Firestore com dados históricos PNCP.

Uso:
    python backfill_firebase.py                          # 2026-01-01 até ontem
    python backfill_firebase.py --start 20260101 --end 20260326
"""

import argparse
import logging
import re
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def _datas_range(start_str: str, end_str: str):
    start = date(int(start_str[:4]), int(start_str[4:6]), int(start_str[6:]))
    end   = date(int(end_str[:4]),   int(end_str[4:6]),   int(end_str[6:]))
    d = start
    while d <= end:
        yield d.strftime("%Y%m%d")
        d += timedelta(days=1)


def _carregar_ou_coletar(data_alvo: str) -> pd.DataFrame | None:
    output_dir = Path(__file__).parent / "output"
    xlsx_path  = output_dir / f"pncp_contratacoes_MT_{data_alvo}.xlsx"

    if not xlsx_path.exists():
        logger.info(f"  Coletando PNCP para {data_alvo}...")
        import asyncio
        from main import run_pipeline
        asyncio.run(run_pipeline(data_inicial=data_alvo, data_final=data_alvo))

    if not xlsx_path.exists():
        logger.warning(f"  Sem dados para {data_alvo} — pulando.")
        return None

    df = pd.read_excel(xlsx_path, dtype=str)
    df["orgaoEntidade_cnpj"] = df["orgaoEntidade_cnpj"].astype(str).apply(
        lambda x: re.sub(r"\D", "", x)
    )
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill Firebase com histórico PNCP")
    parser.add_argument("--start", default="20260101", metavar="YYYYMMDD")
    parser.add_argument("--end",   default=(date.today() - timedelta(days=1)).strftime("%Y%m%d"), metavar="YYYYMMDD")
    args = parser.parse_args()

    from firebase_sync import sincronizar

    datas = list(_datas_range(args.start, args.end))
    logger.info(f"Backfill: {args.start} → {args.end} ({len(datas)} dias)")

    total = {"inseridos": 0, "atualizados": 0, "alertas": 0}

    for i, data_alvo in enumerate(datas, 1):
        logger.info(f"[{i}/{len(datas)}] {data_alvo}")
        df = _carregar_ou_coletar(data_alvo)
        if df is None:
            continue
        r = sincronizar(df, data_alvo)
        total["inseridos"]   += r["inseridos"]
        total["atualizados"] += r["atualizados"]
        total["alertas"]     += r["alertas"]

    print(f"\n=== Backfill concluído ===")
    print(f"  Inseridos:        {total['inseridos']}")
    print(f"  Atualizados:      {total['atualizados']}")
    print(f"  Alertas ativados: {total['alertas']}")

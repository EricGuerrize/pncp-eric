"""
pipeline_multicidades.py — Pipeline PNCP × APLIC para múltiplos municípios.

Orquestra as etapas:
  1. Extração APLIC do Oracle   (aplic_extractor.py)
  2. Coleta PNCP da API          (opcional — usa Excel já existente se disponível)
  3. Crossmatch por município    (crossmatch.py)
  4. Sync para Firebase          (firebase_sync.py)

Uso:
    python pipeline_multicidades.py \\
      --cidades rondolandia acorizal jangada "lucas do rio verde" \\
      --ano 2026

    # Se já tiver o Excel PNCP, pular coleta:
    python pipeline_multicidades.py \\
      --cidades rondolandia acorizal jangada "lucas do rio verde" \\
      --ano 2026 \\
      --pncp-excel output/pncp_contratacoes_MT_20260423.xlsx \\
      --skip-oracle       # pula extração Oracle (usa CSVs já existentes em input/)

    # Só crossmatch + Firebase, sem reprocessar dados:
    python pipeline_multicidades.py \\
      --cidades sinop \\
      --ano 2026 \\
      --pncp-excel output/pncp_contratacoes_MT_20260423.xlsx \\
      --skip-oracle \\
      --skip-firebase
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import unicodedata
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

logger = logging.getLogger(__name__)

INPUT_DIR  = Path(__file__).parent / "input"
OUTPUT_DIR = Path(__file__).parent / "output"
ORGAOS_JSON = INPUT_DIR / "orgaos.json"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalizar(texto: str) -> str:
    nfkd = unicodedata.normalize("NFKD", str(texto))
    return nfkd.encode("ASCII", "ignore").decode("ASCII").upper().strip()


def _slug(nome: str) -> str:
    return _normalizar(nome).lower().replace(" ", "_")


def _encontrar_pncp_excel(pncp_excel_arg: str | None) -> Path | None:
    """Retorna o Excel PNCP a usar: argumento CLI ou o mais recente em output/."""
    if pncp_excel_arg:
        p = Path(pncp_excel_arg)
        if not p.is_absolute():
            p = Path(__file__).parent / p
        if p.exists():
            return p
        logger.error(f"Excel PNCP informado não encontrado: {p}")
        return None

    candidates = sorted(OUTPUT_DIR.glob("pncp_contratacoes_MT_*.xlsx"), reverse=True)
    if candidates:
        logger.info(f"Usando Excel PNCP mais recente: {candidates[0].name}")
        return candidates[0]

    logger.warning("Nenhum Excel PNCP encontrado em output/. Execute main.py primeiro.")
    return None


def _carregar_orgaos() -> list[dict]:
    if ORGAOS_JSON.exists():
        with open(ORGAOS_JSON, encoding="utf-8") as f:
            return json.load(f)
    return []


def _cnpjs_para_municipio(municipio_slug: str) -> set[str]:
    """Retorna o conjunto de CNPJs mapeados para um município em orgaos.json."""
    orgaos = _carregar_orgaos()
    return {
        o["cnpj"]
        for o in orgaos
        if _slug(o.get("municipio", "")) == municipio_slug and o.get("cnpj")
    }


# ---------------------------------------------------------------------------
# Etapa 1: Extração Oracle
# ---------------------------------------------------------------------------

def etapa_oracle(cidades: list[str], ano: int) -> dict[str, Path]:
    """
    Chama aplic_extractor.run() e retorna mapeamento municipio_slug → csv_path.
    """
    from aplic_extractor import run as extrair

    csvs = extrair(cidades, ano)

    mapa: dict[str, Path] = {}
    for csv_path in csvs:
        # Infere slug do nome do arquivo: licitacao_{slug}_{ano}.csv
        stem = csv_path.stem  # ex: licitacao_rondolandia_2026
        parts = stem.split("_", 1)
        if len(parts) == 2:
            slug_com_ano = parts[1]
            slug = "_".join(slug_com_ano.rsplit("_", 1)[:-1])  # remove o ano do final
            mapa[slug] = csv_path

    return mapa


# ---------------------------------------------------------------------------
# Etapa 2: Coleta PNCP (opcional)
# ---------------------------------------------------------------------------

def etapa_coletar_pncp(inicio: str, fim: str) -> Path | None:
    """Chama main.py para coletar PNCP. Retorna o Excel gerado ou None."""
    import subprocess
    logger.info(f"Coletando PNCP: {inicio} → {fim}")
    result = subprocess.run(
        [sys.executable, str(Path(__file__).parent / "main.py"),
         "--from", inicio, "--to", fim],
        cwd=str(Path(__file__).parent),
    )
    if result.returncode != 0:
        logger.error("Falha na coleta PNCP. Verifique main.py.")
        return None
    # Retorna o Excel gerado (o mais recente em output/)
    candidates = sorted(OUTPUT_DIR.glob("pncp_contratacoes_MT_*.xlsx"), reverse=True)
    return candidates[0] if candidates else None


# ---------------------------------------------------------------------------
# Etapa 3: Crossmatch por município
# ---------------------------------------------------------------------------

def etapa_crossmatch(
    municipio: str,
    aplic_csv: Path,
    ano: int,
    pncp_excel: Path | None = None,
) -> Path | None:
    """
    Executa crossmatch para um município e salva Excel de resultado.

    Fonte PNCP (em ordem de preferência):
      1. pncp_excel — arquivo Excel local (quando disponível)
      2. Firebase    — carrega apenas_pncp + ambos do Firestore (padrão quando sem Excel)

    Retorna caminho do Excel de resultado ou None em caso de erro.
    """
    from crossmatch import crossmatch, carregar_aplic, load_orgaos

    load_orgaos()

    municipio_slug = _slug(municipio)
    logger.info(f"[Crossmatch] {municipio_slug}")

    # Carrega PNCP: Excel local ou Firebase
    if pncp_excel is not None:
        df_pncp = pd.read_excel(pncp_excel, dtype=str)
        logger.info(f"  PNCP carregado do Excel: {len(df_pncp)} registros (MT completo)")
    else:
        from firebase_sync import carregar_pncp_municipio
        df_pncp = carregar_pncp_municipio(municipio_slug)
        if df_pncp.empty:
            logger.error(f"  Nenhum dado PNCP no Firebase para {municipio_slug}. "
                         "Execute a sincronização PNCP antes.")
            return None
        logger.info(f"  PNCP carregado do Firebase: {len(df_pncp)} registros ({municipio_slug})")

    # Carrega APLIC do município
    if not aplic_csv.exists():
        logger.error(f"  CSV APLIC não encontrado: {aplic_csv}")
        return None

    df_aplic = carregar_aplic(aplic_csv)
    logger.info(f"  APLIC carregado: {len(df_aplic)} registros ({municipio_slug})")

    if df_aplic.empty:
        logger.warning(f"  APLIC vazio para {municipio_slug}. Pulando.")
        return None

    df_aplic_original = df_aplic.copy()

    # Crossmatch
    df_resultado, df_aplic_grupos = crossmatch(df_pncp, df_aplic)

    if df_resultado.empty:
        logger.warning(f"  Resultado vazio para {municipio_slug}.")
        return None

    # Salva Excel de resultado
    saida = OUTPUT_DIR / f"crossmatch_{municipio_slug}_{ano}.xlsx"
    OUTPUT_DIR.mkdir(exist_ok=True)

    cols_diag = [c for c in [
        "status_cruzamento", "estrategia_match", "score_composto",
        "fuzzy_score", "delta_percentual", "delta_dias", "validacao_financeira",
    ] if c in df_resultado.columns]
    outras = [c for c in df_resultado.columns if c not in cols_diag]
    df_resultado = df_resultado[cols_diag + outras]

    with pd.ExcelWriter(saida, engine="openpyxl") as writer:
        df_resultado.to_excel(writer, sheet_name="Resultados", index=False)

        if not df_aplic_grupos.empty:
            cols_aux = [c for c in df_aplic_grupos.columns
                        if c.startswith("_") and c not in ("_dedup_tipo", "_dedup_grupo")]
            df_dup = df_aplic_grupos.drop(columns=cols_aux, errors="ignore").rename(columns={
                "_dedup_tipo":  "Tipo (principal/duplicata)",
                "_dedup_grupo": "Grupo",
            })
            id_cols = ["Grupo", "Tipo (principal/duplicata)"]
            df_dup[id_cols + [c for c in df_dup.columns if c not in id_cols]].to_excel(
                writer, sheet_name="APLIC_Duplicatas", index=False
            )

        df_aplic_original.to_excel(writer, sheet_name="APLIC_Completo", index=False)

        linhas_resumo = []
        for col in ("status_cruzamento", "estrategia_match"):
            if col in df_resultado.columns:
                for val, qtd in df_resultado[col].value_counts().items():
                    linhas_resumo.append({"Categoria": col, "Valor": val, "Qtd": qtd})
        pd.DataFrame(linhas_resumo).to_excel(writer, sheet_name="Resumo", index=False)

    logger.info(f"  Resultado salvo: {saida.name} ({len(df_resultado)} linhas)")

    if "status_cruzamento" in df_resultado.columns:
        for status, qtd in df_resultado["status_cruzamento"].value_counts().items():
            logger.info(f"    {status}: {qtd}")

    return saida


# ---------------------------------------------------------------------------
# Etapa 2b: Sync PNCP completo para Firebase (apenas_pncp de todos os municípios)
# ---------------------------------------------------------------------------

def etapa_sincronizar_pncp(pncp_excel: Path) -> dict:
    """
    Lê o Excel PNCP completo de MT e popula o Firebase com TODAS as licitações
    em 'apenas_pncp' para todos os municípios presentes no arquivo.
    Este passo deve rodar ANTES do crossmatch.
    """
    from firebase_sync import sincronizar

    logger.info(f"[Firebase] Sincronizando PNCP completo: {pncp_excel.name}")
    df_pncp = pd.read_excel(pncp_excel, dtype=str)
    logger.info(f"  {len(df_pncp)} licitações carregadas do Excel PNCP")

    data_ref = pncp_excel.stem.split("_")[-1]  # ex: "20260423" do nome do arquivo
    stats = sincronizar(df_pncp, data_ref=data_ref)
    logger.info(
        f"  Sync PNCP concluído: "
        f"{stats.get('inseridos', 0)} inseridos, "
        f"{stats.get('atualizados', 0)} atualizados, "
        f"{stats.get('alertas', 0)} alertas"
    )
    return stats


# ---------------------------------------------------------------------------
# Etapa 4: Sync Firebase (crossmatch)
# ---------------------------------------------------------------------------

def etapa_firebase(crossmatch_excel: Path, municipio: str) -> dict:
    """Sincroniza resultado do crossmatch para Firestore (ambos + apenas_aplic)."""
    from firebase_sync import sincronizar_crossmatch

    logger.info(f"[Firebase] {_slug(municipio)} ← {crossmatch_excel.name}")
    df = pd.read_excel(crossmatch_excel, sheet_name="Resultados", dtype=str)
    return sincronizar_crossmatch(df, municipio=municipio)


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

def run(
    cidades: list[str],
    ano: int,
    pncp_excel: str | None = None,
    skip_oracle: bool = False,
    skip_firebase: bool = False,
    skip_pncp_sync: bool = False,
    pncp_inicio: str | None = None,
    pncp_fim: str | None = None,
) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # ── Passo 1: Excel PNCP (opcional) ───────────────────────────────────────
    pncp_path = _encontrar_pncp_excel(pncp_excel)

    if pncp_path is None and pncp_inicio and pncp_fim:
        logger.info("Coletando PNCP via main.py...")
        pncp_path = etapa_coletar_pncp(pncp_inicio, pncp_fim)

    if pncp_path is None:
        logger.info(
            "Excel PNCP não encontrado — PNCP será lido do Firebase por município. "
            "Use --pncp-excel ou --pncp-inicio/--pncp-fim para usar um arquivo local."
        )

    # ── Passo 2: Sync PNCP completo → Firebase (todos os municípios MT) ────────
    if pncp_path is not None and not skip_firebase and not skip_pncp_sync:
        logger.info("Sincronizando PNCP completo com Firebase...")
        etapa_sincronizar_pncp(pncp_path)
    elif pncp_path is None:
        logger.info("Sync PNCP pulado (sem Excel local — usando Firebase como fonte)")
    else:
        logger.info("Sync PNCP pulado (--skip-firebase ou --skip-pncp-sync)")

    # ── Passo 3: Extração Oracle ─────────────────────────────────────────────
    aplic_csvs: dict[str, Path] = {}

    if not skip_oracle:
        aplic_csvs = etapa_oracle(cidades, ano)
    else:
        logger.info("--skip-oracle: usando CSVs existentes em input/")
        for cidade in cidades:
            slug = _slug(cidade)
            candidate = INPUT_DIR / f"licitacao_{slug}_{ano}.csv"
            if candidate.exists():
                aplic_csvs[slug] = candidate
                logger.info(f"  Encontrado: {candidate.name}")
            else:
                logger.warning(f"  CSV não encontrado: {candidate.name}")

    if not aplic_csvs:
        logger.error("Nenhum CSV APLIC disponível. Verifique a conexão Oracle ou os arquivos em input/.")
        sys.exit(1)

    # ── Passos 3+4: Crossmatch → Firebase por cidade ─────────────────────────
    resultados: list[dict] = []

    for cidade in cidades:
        slug = _slug(cidade)
        aplic_csv = aplic_csvs.get(slug)

        if aplic_csv is None:
            logger.warning(f"Sem CSV APLIC para '{cidade}' (slug: {slug}). Pulando.")
            continue

        # Crossmatch (pncp_path pode ser None → carrega do Firebase)
        crossmatch_xlsx = etapa_crossmatch(cidade, aplic_csv, ano, pncp_excel=pncp_path)
        if crossmatch_xlsx is None:
            continue

        # Firebase
        if not skip_firebase:
            stats = etapa_firebase(crossmatch_xlsx, municipio=cidade)
            resultados.append({"municipio": slug, **stats})
        else:
            logger.info(f"--skip-firebase: sync pulado para {slug}")

    # ── Resumo final ──────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("PIPELINE MULTICIDADES — RESUMO")
    print("=" * 60)
    for r in resultados:
        print(
            f"  {r['municipio']:30s}  "
            f"ambos: {r.get('movidos_para_ambos', 0):4d}  "
            f"apenas_aplic: {r.get('inseridos_apenas_aplic', 0):4d}"
        )
    print("=" * 60)
    print(f"  {len(resultados)} município(s) processado(s).")
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Pipeline PNCP × APLIC para múltiplos municípios."
    )
    parser.add_argument(
        "--cidades",
        nargs="+",
        required=True,
        help="Municípios a processar (ex: rondolandia acorizal 'lucas do rio verde')",
    )
    parser.add_argument(
        "--ano",
        type=int,
        default=2026,
        help="Exercício (padrão: 2026)",
    )
    parser.add_argument(
        "--pncp-excel",
        metavar="ARQUIVO",
        help="Caminho para o Excel PNCP. Se omitido, usa o mais recente em output/",
    )
    parser.add_argument(
        "--pncp-inicio",
        metavar="YYYYMMDD",
        help="Data início para coleta PNCP (ex: 20260101). Ativa coleta via main.py",
    )
    parser.add_argument(
        "--pncp-fim",
        metavar="YYYYMMDD",
        help="Data fim para coleta PNCP (ex: 20260423)",
    )
    parser.add_argument(
        "--skip-oracle",
        action="store_true",
        help="Pula extração Oracle; usa CSVs já existentes em input/",
    )
    parser.add_argument(
        "--skip-firebase",
        action="store_true",
        help="Pula todo sync para Firebase (apenas gera Excel de crossmatch)",
    )
    parser.add_argument(
        "--skip-pncp-sync",
        action="store_true",
        help="Pula re-sincronização do PNCP completo (use quando Firebase já está atualizado)",
    )

    args = parser.parse_args()

    run(
        cidades=args.cidades,
        ano=args.ano,
        pncp_excel=args.pncp_excel,
        skip_oracle=args.skip_oracle,
        skip_firebase=args.skip_firebase,
        skip_pncp_sync=args.skip_pncp_sync,
        pncp_inicio=args.pncp_inicio,
        pncp_fim=args.pncp_fim,
    )


if __name__ == "__main__":
    main()

"""
firebase_sync.py — Sincroniza registros PNCP com o Firestore para todos os municípios de MT.

Uso standalone:
    python firebase_sync.py                        # D-1 automático
    python firebase_sync.py --date 20260325        # data específica
    python firebase_sync.py --sync-aplic <xlsx> --municipio sinop  # crossmatch para um município

Estrutura no Firestore:
    municipios/
      {municipio_slug}/          ← ex: sinop, cuiaba, lucas_do_rio_verde
        apenas_pncp/
          {id}/
            municipio, orgao, modalidade, numero, ano, objeto, valor, cnpj
            dataPNCP, prazoAplic
            statusPNCP: "S", statusAPLIC: "pendente"
            alertaAtivo: false → true quando prazoAplic vencer
            criadoEm, atualizadoEm

        apenas_aplic/
          {id}/
            municipio, orgao, modalidade, numero, ano, objeto, valor, cnpj
            dataAPLIC
            statusPNCP: "N", statusAPLIC: "S"
            criadoEm, atualizadoEm

        ambos/
          {id}/
            (todos os campos acima + campos APLIC)
            statusPNCP: "S", statusAPLIC: "S"
            score_cruzamento, estrategia_match
            criadoEm, atualizadoEm
"""

import argparse
import logging
import re
import unicodedata
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

CREDENTIALS_PATH   = Path(__file__).resolve().parent.parent / "firebase_credentials.json"
COLECAO_MUNICIPIOS = "municipios"

SUB_APENAS_PNCP  = "apenas_pncp"
SUB_APENAS_APLIC = "apenas_aplic"
SUB_AMBOS        = "ambos"

COL_MUNICIPIO = "unidadeOrgao_municipioNome"


# ---------------------------------------------------------------------------
# Utilitários
# ---------------------------------------------------------------------------

def _slug_municipio(nome: str) -> str:
    """Normaliza nome do município para uso como ID no Firestore."""
    nfkd = unicodedata.normalize("NFKD", nome.lower().strip())
    sem_acento = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", "_", sem_acento).strip("_")


def _adicionar_dias_uteis(data: datetime, dias: int) -> datetime:
    atual = data
    adicionados = 0
    while adicionados < dias:
        atual += timedelta(days=1)
        if atual.weekday() < 5:
            adicionados += 1
    return atual


def _dt(val):
    if not val or str(val).strip() in ("", "nan"):
        return None
    try:
        d = pd.to_datetime(val, errors="coerce")
        if pd.isna(d):
            return None
        d = d.to_pydatetime()
        if hasattr(d, "tzinfo") and d.tzinfo:
            d = d.replace(tzinfo=None)
        return d
    except Exception:
        return None


def _fval(val):
    if not val or str(val).strip() in ("", "nan"):
        return None
    try:
        return float(str(val).replace("R$", "").replace(".", "").replace(",", ".").strip())
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Firebase
# ---------------------------------------------------------------------------

def _inicializar_firebase():
    import firebase_admin
    from firebase_admin import credentials, firestore

    if not CREDENTIALS_PATH.exists():
        raise FileNotFoundError(
            f"Credencial não encontrada: {CREDENTIALS_PATH}\n"
            "Coloque o arquivo firebase_credentials.json na raiz do projeto."
        )

    if not firebase_admin._apps:
        cred = credentials.Certificate(str(CREDENTIALS_PATH))
        firebase_admin.initialize_app(cred)

    return firestore.client()


def _sub(db, municipio_slug: str, colecao: str):
    return (
        db.collection(COLECAO_MUNICIPIOS)
          .document(municipio_slug)
          .collection(colecao)
    )


# ---------------------------------------------------------------------------
# Conversores de linha → documento
# ---------------------------------------------------------------------------

def _doc_pncp(row: pd.Series, municipio_slug: str) -> dict:
    from google.cloud.firestore_v1 import SERVER_TIMESTAMP

    data_pncp = _dt(row.get("dataPublicacaoPncp") or row.get("dataAberturaProposta"))
    if not data_pncp:
        data_pncp = datetime.utcnow()

    prazo_aplic = _adicionar_dias_uteis(data_pncp, 3)
    valor = _fval(row.get("valorTotalEstimado") or row.get("valorTotalHomologado"))

    return {
        "municipio":    municipio_slug,
        "orgao":        str(row.get("unidadeOrgao_nomeUnidade") or "")[:80],
        "modalidade":   str(row.get("modalidadeNome") or "")[:60],
        "numero":       str(row.get("numeroCompra") or ""),
        "ano":          str(row.get("anoCompra") or ""),
        "objeto":       str(row.get("objetoCompra") or "")[:300],
        "valor":        valor,
        "cnpj":         re.sub(r"\D", "", str(row.get("orgaoEntidade_cnpj") or "")),
        "dataPNCP":     data_pncp,
        "prazoAplic":   prazo_aplic,
        "statusPNCP":   "S",
        "atualizadoEm": SERVER_TIMESTAMP,
    }


def _doc_aplic(row: pd.Series, municipio_slug: str) -> dict:
    from google.cloud.firestore_v1 import SERVER_TIMESTAMP

    return {
        "municipio":    municipio_slug,
        "orgao":        str(row.get("_orgao_nome") or row.get("Órgão") or "")[:80],
        "modalidade":   str(row.get("_modalidade_pncp") or row.get("Modalidade") or "")[:60],
        "numero":       str(row.get("_numero_aplic") or row.get("Número") or ""),
        "ano":          str(row.get("_ano_aplic") or row.get("Ano") or ""),
        "objeto":       str(row.get("_objetivo_norm") or row.get("Objetivo") or row.get("Motivo") or "")[:300],
        "valor":        _fval(row.get("Valor Estimado")),
        "cnpj":         str(row.get("_cnpj_mapeado") or ""),
        "dataAPLIC":    _dt(row.get("dataAberturaAplic") or row.get("dataAbertura") or ""),
        "statusPNCP":   "N",
        "statusAPLIC":  "S",
        "atualizadoEm": SERVER_TIMESTAMP,
    }


# ---------------------------------------------------------------------------
# Sincronização PNCP D-1 → apenas_pncp (todos os municípios)
# ---------------------------------------------------------------------------

def sincronizar(df_pncp: pd.DataFrame, data_ref: str = "") -> dict:
    """
    Itera todos os municípios presentes no DataFrame e sincroniza cada um
    com sua respectiva subcoleção no Firestore.
    """
    from google.cloud.firestore_v1 import SERVER_TIMESTAMP

    if COL_MUNICIPIO not in df_pncp.columns:
        logger.error(f"Coluna '{COL_MUNICIPIO}' não encontrada no DataFrame")
        return {"inseridos": 0, "atualizados": 0, "alertas": 0}

    db = _inicializar_firebase()
    hoje = datetime.utcnow()

    total_inseridos = total_atualizados = total_alertas = 0
    municipios_processados = 0

    for municipio_raw, df_mun in df_pncp.groupby(COL_MUNICIPIO):
        municipio_slug = _slug_municipio(str(municipio_raw))

        # Cria/atualiza documento pai para que o município apareça ao listar a coleção
        from google.cloud.firestore_v1 import SERVER_TIMESTAMP as _STS
        db.collection(COLECAO_MUNICIPIOS).document(municipio_slug).set(
            {"nome": str(municipio_raw), "slug": municipio_slug, "ultimaSync": _STS},
            merge=True,
        )

        col_apenas_pncp = _sub(db, municipio_slug, SUB_APENAS_PNCP)
        col_ambos       = _sub(db, municipio_slug, SUB_AMBOS)

        inseridos = atualizados = alertas = 0

        for _, row in df_mun.iterrows():
            doc_id = str(row.get("numeroControlePNCP") or "").strip().replace("/", "_")
            if not doc_id:
                continue

            if col_ambos.document(doc_id).get().exists:
                continue

            ref  = col_apenas_pncp.document(doc_id)
            snap = ref.get()
            doc  = _doc_pncp(row, municipio_slug)

            if not snap.exists:
                doc["statusAPLIC"] = "pendente"
                doc["alertaAtivo"] = False
                doc["criadoEm"]    = SERVER_TIMESTAMP
                ref.set(doc)
                inseridos += 1
            else:
                dados = snap.to_dict()
                prazo = doc["prazoAplic"]
                alerta_ativo = (dados.get("statusAPLIC") == "pendente") and (hoje > prazo)
                if alerta_ativo and not dados.get("alertaAtivo", False):
                    alertas += 1
                    logger.warning(f"  [!] PRAZO VENCIDO: {municipio_slug} | {doc['orgao']} | {doc['numero']}")
                doc["alertaAtivo"] = alerta_ativo
                ref.update(doc)
                atualizados += 1

        if inseridos or atualizados or alertas:
            logger.info(
                f"  [{municipio_raw}] {inseridos} inseridos | "
                f"{atualizados} atualizados | {alertas} alertas"
            )

        total_inseridos  += inseridos
        total_atualizados += atualizados
        total_alertas    += alertas
        municipios_processados += 1

    logger.info(
        f"[Firebase] {municipios_processados} municípios processados | "
        f"TOTAL: {total_inseridos} inseridos | "
        f"{total_atualizados} atualizados | {total_alertas} alertas"
    )
    return {
        "inseridos":              total_inseridos,
        "atualizados":            total_atualizados,
        "alertas":                total_alertas,
        "municipios_processados": municipios_processados,
    }


# ---------------------------------------------------------------------------
# Sincronização crossmatch → ambos + apenas_aplic (por município)
# ---------------------------------------------------------------------------

def sincronizar_crossmatch(df_crossmatch: pd.DataFrame, municipio: str = "sinop") -> dict:
    """
    Processa resultado do crossmatch para um município específico:

    - MATCH_CONFIRMADO / MATCH_PARCIAL → move apenas_pncp → ambos
    - APENAS_APLIC                     → insere em apenas_aplic
    - APENAS_PNCP                      → já está em apenas_pncp, sem ação
    """
    from google.cloud.firestore_v1 import SERVER_TIMESTAMP

    municipio_slug   = _slug_municipio(municipio)
    db               = _inicializar_firebase()
    col_apenas_pncp  = _sub(db, municipio_slug, SUB_APENAS_PNCP)
    col_apenas_aplic = _sub(db, municipio_slug, SUB_APENAS_APLIC)
    col_ambos        = _sub(db, municipio_slug, SUB_AMBOS)

    movidos = inseridos_aplic = 0

    for _, row in df_crossmatch.iterrows():
        status = str(row.get("status_cruzamento") or "")

        if status in ("MATCH_CONFIRMADO", "MATCH_PARCIAL"):
            doc_id = str(row.get("numeroControlePNCP") or "").strip().replace("/", "_")
            if not doc_id:
                continue

            snap_pncp  = col_apenas_pncp.document(doc_id).get()
            dados_base = snap_pncp.to_dict() if snap_pncp.exists else {}

            data_pncp   = _dt(row.get("dataPublicacaoPncp") or row.get("dataAberturaProposta"))
            prazo_aplic = _adicionar_dias_uteis(data_pncp, 3) if data_pncp else None

            doc_ambos = {
                **dados_base,
                "municipio":        municipio_slug,
                "orgao":            str(row.get("unidadeOrgao_nomeUnidade") or dados_base.get("orgao", ""))[:80],
                "modalidade":       str(row.get("modalidadeNome") or dados_base.get("modalidade", ""))[:60],
                "numero":           str(row.get("numeroCompra") or dados_base.get("numero", "")),
                "ano":              str(row.get("anoCompra") or dados_base.get("ano", "")),
                "objeto":           str(row.get("objetoCompra") or dados_base.get("objeto", ""))[:300],
                "valor":            _fval(row.get("valorTotalEstimado") or row.get("valorTotalHomologado")) or dados_base.get("valor"),
                "cnpj":             re.sub(r"\D", "", str(row.get("orgaoEntidade_cnpj") or dados_base.get("cnpj", ""))),
                "dataPNCP":         data_pncp or dados_base.get("dataPNCP"),
                "prazoAplic":       prazo_aplic or dados_base.get("prazoAplic"),
                "statusPNCP":       "S",
                "orgao_aplic":      str(row.get("UG") or row.get("_orgao_nome") or "")[:80],
                "numero_aplic":     str(row.get("Nº Licitação") or ""),
                "objeto_aplic":     str(row.get("Objetivo") or row.get("Motivo") or row.get("_objetivo_norm") or "")[:300],
                "valor_aplic":      _fval(row.get("Valor Estimado")),
                "dataAPLIC":        _dt(row.get("Data Abertura")),
                "statusAPLIC":      "S",
                "alertaAtivo":      False,
                "score_cruzamento": str(row.get("score_composto") or ""),
                "estrategia_match": str(row.get("estrategia_match") or ""),
                "atualizadoEm":     SERVER_TIMESTAMP,
            }

            col_ambos.document(doc_id).set(doc_ambos)

            if snap_pncp.exists:
                col_apenas_pncp.document(doc_id).delete()

            movidos += 1
            logger.info(f"  [→ ambos] {doc_id}")

        elif status == "APENAS_APLIC":
            cnpj   = str(row.get("_cnpj_mapeado") or "").replace("/", "_")
            numero = str(row.get("_numero_aplic") or "").replace("/", "_")
            ano    = str(row.get("_ano_aplic") or "")
            doc_id = f"{cnpj}-{numero}-{ano}" if cnpj else ""
            if not doc_id:
                continue

            ref  = col_apenas_aplic.document(doc_id)
            snap = ref.get()
            doc  = _doc_aplic(row, municipio_slug)

            if not snap.exists:
                doc["criadoEm"] = SERVER_TIMESTAMP
                ref.set(doc)
                inseridos_aplic += 1
                logger.info(f"  [+ aplic] {doc['orgao']} | {doc['numero']}")
            else:
                ref.update(doc)

    logger.info(
        f"[Firebase Crossmatch] {municipio_slug}: "
        f"Movidos para ambos: {movidos} | Inseridos apenas_aplic: {inseridos_aplic}"
    )
    return {"movidos_para_ambos": movidos, "inseridos_apenas_aplic": inseridos_aplic}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    parser = argparse.ArgumentParser(description="Sincroniza PNCP com Firebase (todos os municípios)")
    parser.add_argument("--date", metavar="YYYYMMDD",
                        help="Data a processar (padrão: ontem)")
    parser.add_argument("--sync-aplic", metavar="XLSX",
                        help="Excel de crossmatch para atualizar Firestore")
    parser.add_argument("--municipio", default="sinop",
                        help="Município alvo do crossmatch (padrão: sinop)")
    args = parser.parse_args()

    if args.sync_aplic:
        logger.info(f"Processando crossmatch ({args.municipio}): {args.sync_aplic}")
        df_cross = pd.read_excel(args.sync_aplic, sheet_name="Resultados", dtype=str)
        resultado = sincronizar_crossmatch(df_cross, municipio=args.municipio)
        print(f"\nFirestore atualizado com crossmatch ({args.municipio}):")
        print(f"  Movidos para ambos:      {resultado['movidos_para_ambos']}")
        print(f"  Inseridos apenas_aplic:  {resultado['inseridos_apenas_aplic']}")
        raise SystemExit(0)

    data_alvo = args.date or (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    logger.info(f"Data alvo: {data_alvo} — processando todos os municípios de MT")

    xlsx_path = Path(__file__).parent / "output" / f"pncp_contratacoes_MT_{data_alvo}.xlsx"

    if not xlsx_path.exists():
        logger.info("Excel não encontrado — rodando pipeline de coleta...")
        import asyncio
        import sys
        sys.path.insert(0, str(Path(__file__).parent))
        from main import run_pipeline
        asyncio.run(run_pipeline(data_inicial=data_alvo, data_final=data_alvo))

    if not xlsx_path.exists():
        logger.error(f"Pipeline não gerou o arquivo esperado: {xlsx_path}")
        raise SystemExit(1)

    logger.info(f"Carregando: {xlsx_path}")
    df = pd.read_excel(xlsx_path, dtype=str)
    df["orgaoEntidade_cnpj"] = df["orgaoEntidade_cnpj"].astype(str).apply(
        lambda x: re.sub(r"\D", "", x)
    )

    resultado = sincronizar(df, data_alvo)
    print(f"\nFirestore atualizado — {data_alvo}")
    print(f"  Municípios processados: {resultado['municipios_processados']}")
    print(f"  Inseridos:              {resultado['inseridos']}")
    print(f"  Atualizados:            {resultado['atualizados']}")
    print(f"  Alertas ativados:       {resultado['alertas']}")

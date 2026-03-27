"""
firebase_sync.py — Sincroniza registros PNCP/APLIC com o Firestore.

Uso standalone:
    python firebase_sync.py                        # D-1 automático
    python firebase_sync.py --date 20260325        # data específica
    python firebase_sync.py --sync-aplic <xlsx>    # atualiza com resultado do crossmatch

Estrutura no Firestore:
    municipios/
      sinop/
        apenas_pncp/      ← publicado no PNCP, ainda sem registro no APLIC
          {id}/
            orgao, modalidade, numero, ano, objeto, valor, cnpj
            dataPNCP, prazoAplic (dataPNCP + 3 dias úteis)
            statusPNCP: "S", statusAPLIC: "pendente"
            alertaAtivo: false → true quando prazoAplic vencer
            criadoEm, atualizadoEm

        apenas_aplic/     ← existe no APLIC mas não foi publicado no PNCP
          {id}/
            orgao, modalidade, numero, ano, objeto, valor, cnpj
            dataAPLIC
            statusPNCP: "N", statusAPLIC: "S"
            criadoEm, atualizadoEm

        ambos/            ← matched nos dois sistemas
          {id}/
            (todos os campos acima + campos APLIC)
            statusPNCP: "S", statusAPLIC: "S"
            score_cruzamento, estrategia_match
            criadoEm, atualizadoEm
"""

import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

CREDENTIALS_PATH   = Path(__file__).resolve().parent.parent / "firebase_credentials.json"
COLECAO_MUNICIPIOS = "municipios"   # coleção raiz: um doc por cidade

# Subcoleções dentro de cada município
SUB_APENAS_PNCP  = "apenas_pncp"
SUB_APENAS_APLIC = "apenas_aplic"
SUB_AMBOS        = "ambos"

# CNPJs do município a monitorar (Sinop) — espelha DE_PARA_UG_CNPJ do crossmatch
MUNICIPIO_NOME = "sinop"
CNPJS_MUNICIPIO = {
    "00814574000101",  # Câmara Municipal de Sinop
    "00571071000144",  # Instituto de Previdência de Sinop
    "15024003000132",  # Prefeitura Municipal de Sinop
}


# ---------------------------------------------------------------------------
# Firebase
# ---------------------------------------------------------------------------

def _inicializar_firebase():
    """Inicializa o app Firebase a partir do arquivo de credenciais."""
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


def _sub(db, nome: str):
    """Atalho para acessar uma subcoleção do município."""
    return (db.collection(COLECAO_MUNICIPIOS)
              .document(MUNICIPIO_NOME)
              .collection(nome))


# ---------------------------------------------------------------------------
# Dias úteis
# ---------------------------------------------------------------------------

def _adicionar_dias_uteis(data: datetime, dias: int) -> datetime:
    """Adiciona N dias úteis a uma data (ignora sábado e domingo)."""
    atual = data
    adicionados = 0
    while adicionados < dias:
        atual += timedelta(days=1)
        if atual.weekday() < 5:   # 0=seg … 4=sex
            adicionados += 1
    return atual


# ---------------------------------------------------------------------------
# Conversores de linha → documento
# ---------------------------------------------------------------------------

def _doc_pncp(row: pd.Series) -> dict:
    """Converte uma linha do DataFrame PNCP em dicionário Firestore."""
    from google.cloud.firestore_v1 import SERVER_TIMESTAMP

    data_pncp_raw = row.get("dataPublicacaoPncp") or row.get("dataAberturaProposta")
    try:
        data_pncp = pd.to_datetime(data_pncp_raw, errors="coerce")
        data_pncp = data_pncp.to_pydatetime() if pd.notna(data_pncp) else datetime.utcnow()
        if hasattr(data_pncp, "tzinfo") and data_pncp.tzinfo:
            data_pncp = data_pncp.replace(tzinfo=None)
    except Exception:
        data_pncp = datetime.utcnow()

    prazo_aplic = _adicionar_dias_uteis(data_pncp, 3)

    valor_raw = row.get("valorTotalEstimado") or row.get("valorTotalHomologado")
    try:
        valor = float(valor_raw) if valor_raw and str(valor_raw).strip() not in ("", "nan") else None
    except (ValueError, TypeError):
        valor = None

    return {
        "municipio":    MUNICIPIO_NOME,
        "orgao":        str(row.get("unidadeOrgao_nomeUnidade") or "")[:80],
        "modalidade":   str(row.get("modalidadeNome") or "")[:60],
        "numero":       str(row.get("numeroCompra") or ""),
        "ano":          str(row.get("anoCompra") or ""),
        "objeto":       str(row.get("objetoCompra") or "")[:300],
        "valor":        valor,
        "cnpj":         str(row.get("orgaoEntidade_cnpj") or ""),
        "dataPNCP":     data_pncp,
        "prazoAplic":   prazo_aplic,
        "statusPNCP":   "S",
        "atualizadoEm": SERVER_TIMESTAMP,
    }


def _doc_aplic(row: pd.Series) -> dict:
    """Converte uma linha APENAS_APLIC do crossmatch em dicionário Firestore."""
    from google.cloud.firestore_v1 import SERVER_TIMESTAMP

    data_raw = row.get("dataAberturaAplic") or row.get("dataAbertura") or ""
    try:
        data_aplic = pd.to_datetime(data_raw, errors="coerce")
        data_aplic = data_aplic.to_pydatetime() if pd.notna(data_aplic) else None
        if data_aplic and hasattr(data_aplic, "tzinfo") and data_aplic.tzinfo:
            data_aplic = data_aplic.replace(tzinfo=None)
    except Exception:
        data_aplic = None

    valor_raw = row.get("Valor Estimado") or row.get("valorEstimado") or ""
    try:
        valor = float(str(valor_raw).replace("R$", "").replace(".", "").replace(",", ".").strip()) if valor_raw and str(valor_raw).strip() not in ("", "nan") else None
    except (ValueError, TypeError):
        valor = None

    return {
        "municipio":    MUNICIPIO_NOME,
        "orgao":        str(row.get("_orgao_nome") or row.get("Órgão") or "")[:80],
        "modalidade":   str(row.get("_modalidade_pncp") or row.get("Modalidade") or "")[:60],
        "numero":       str(row.get("_numero_aplic") or row.get("Número") or ""),
        "ano":          str(row.get("_ano_aplic") or row.get("Ano") or ""),
        "objeto":       str(row.get("_objetivo_norm") or row.get("Objetivo") or row.get("Motivo") or "")[:300],
        "valor":        valor,
        "cnpj":         str(row.get("_cnpj_mapeado") or ""),
        "dataAPLIC":    data_aplic,
        "statusPNCP":   "N",
        "statusAPLIC":  "S",
        "atualizadoEm": SERVER_TIMESTAMP,
    }


# ---------------------------------------------------------------------------
# Sincronização PNCP D-1 → apenas_pncp
# ---------------------------------------------------------------------------

def sincronizar(df_pncp: pd.DataFrame, data_ref: str) -> dict:
    """
    Insere registros do dia em apenas_pncp com statusAPLIC=pendente.
    Registros já existentes em ambos/ não são duplicados.
    """
    from google.cloud.firestore_v1 import SERVER_TIMESTAMP

    db = _inicializar_firebase()
    col_apenas_pncp = _sub(db, SUB_APENAS_PNCP)
    col_ambos       = _sub(db, SUB_AMBOS)

    mask = df_pncp["orgaoEntidade_cnpj"].astype(str).isin(CNPJS_MUNICIPIO)
    df_municipio = df_pncp[mask].copy()

    logger.info(f"[Firebase] {len(df_municipio)} registros de {MUNICIPIO_NOME} para sincronizar")

    inseridos = atualizados = alertas = 0
    hoje = datetime.utcnow()

    for _, row in df_municipio.iterrows():
        doc_id = str(row.get("numeroControlePNCP") or "").strip().replace("/", "_")
        if not doc_id:
            continue

        # Se já está em ambos/, não regride para apenas_pncp
        if col_ambos.document(doc_id).get().exists:
            continue

        ref  = col_apenas_pncp.document(doc_id)
        snap = ref.get()
        doc  = _doc_pncp(row)

        if not snap.exists:
            doc["statusAPLIC"] = "pendente"
            doc["alertaAtivo"] = False
            doc["criadoEm"]    = SERVER_TIMESTAMP
            ref.set(doc)
            inseridos += 1
            logger.info(f"  [+] {doc['orgao']} | {doc['numero']} | {doc['objeto'][:60]}")
        else:
            dados_existentes = snap.to_dict()
            status_aplic = dados_existentes.get("statusAPLIC", "pendente")
            prazo = doc["prazoAplic"]
            alerta_ativo = (status_aplic == "pendente") and (hoje > prazo)
            if alerta_ativo and not dados_existentes.get("alertaAtivo", False):
                alertas += 1
                logger.warning(f"  [!] PRAZO VENCIDO: {doc['orgao']} | {doc['numero']}")
            doc["alertaAtivo"] = alerta_ativo
            ref.update(doc)
            atualizados += 1

    logger.info(
        f"[Firebase] Concluído: {inseridos} inseridos | "
        f"{atualizados} atualizados | {alertas} alertas ativados"
    )
    return {"inseridos": inseridos, "atualizados": atualizados, "alertas": alertas}


# ---------------------------------------------------------------------------
# Sincronização crossmatch → ambos + apenas_aplic
# ---------------------------------------------------------------------------

def sincronizar_crossmatch(df_crossmatch: pd.DataFrame) -> dict:
    """
    Processa o resultado completo do crossmatch:

    - MATCH_CONFIRMADO / MATCH_PARCIAL:
        Move o doc de apenas_pncp → ambos, atualiza statusAPLIC = "S"

    - APENAS_APLIC:
        Insere na subcoleção apenas_aplic com statusPNCP = "N"

    - APENAS_PNCP:
        Mantém em apenas_pncp, não faz nada (já está lá)
    """
    from google.cloud.firestore_v1 import SERVER_TIMESTAMP

    db = _inicializar_firebase()
    col_apenas_pncp  = _sub(db, SUB_APENAS_PNCP)
    col_apenas_aplic = _sub(db, SUB_APENAS_APLIC)
    col_ambos        = _sub(db, SUB_AMBOS)

    movidos = inseridos_aplic = nao_encontrados = 0

    for _, row in df_crossmatch.iterrows():
        status = str(row.get("status_cruzamento") or "")

        # ── Matched: move apenas_pncp → ambos ──────────────────────────────
        if status in ("MATCH_CONFIRMADO", "MATCH_PARCIAL"):
            doc_id = str(row.get("numeroControlePNCP") or "").strip().replace("/", "_")
            if not doc_id:
                continue

            snap_pncp = col_apenas_pncp.document(doc_id).get()
            dados_base = snap_pncp.to_dict() if snap_pncp.exists else {}

            doc_ambos = {
                **dados_base,
                "statusAPLIC":       "S",
                "alertaAtivo":       False,
                "score_cruzamento":  row.get("score_composto", ""),
                "estrategia_match":  str(row.get("estrategia_match") or ""),
                "orgao_aplic":       str(row.get("_orgao_nome") or "")[:80],
                "objeto_aplic":      str(row.get("_objetivo_norm") or "")[:300],
                "atualizadoEm":      SERVER_TIMESTAMP,
            }

            col_ambos.document(doc_id).set(doc_ambos)

            # Remove de apenas_pncp após mover
            if snap_pncp.exists:
                col_apenas_pncp.document(doc_id).delete()

            movidos += 1
            logger.info(f"  [→ ambos] {doc_id}")

        # ── Apenas APLIC: insere em apenas_aplic ───────────────────────────
        elif status == "APENAS_APLIC":
            # ID baseado em cnpj + numero + ano (sem numeroControlePNCP)
            cnpj   = str(row.get("_cnpj_mapeado") or "").replace("/", "_")
            numero = str(row.get("_numero_aplic") or "").replace("/", "_")
            ano    = str(row.get("_ano_aplic") or "")
            doc_id = f"{cnpj}-{numero}-{ano}" if cnpj else ""
            if not doc_id:
                continue

            ref  = col_apenas_aplic.document(doc_id)
            snap = ref.get()
            doc  = _doc_aplic(row)

            if not snap.exists:
                doc["criadoEm"] = SERVER_TIMESTAMP
                ref.set(doc)
                inseridos_aplic += 1
                logger.info(f"  [+ aplic] {doc['orgao']} | {doc['numero']} | {doc['objeto'][:60]}")
            else:
                ref.update(doc)

    logger.info(
        f"[Firebase Crossmatch] Movidos para ambos: {movidos} | "
        f"Inseridos apenas_aplic: {inseridos_aplic}"
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

    parser = argparse.ArgumentParser(description="Sincroniza PNCP/APLIC com Firebase")
    parser.add_argument("--date", metavar="YYYYMMDD",
                        help="Data a processar (padrão: ontem)")
    parser.add_argument("--sync-aplic", metavar="XLSX",
                        help="Excel de crossmatch para atualizar Firestore (matched + apenas_aplic)")
    args = parser.parse_args()

    # Modo crossmatch: processa resultado completo
    if args.sync_aplic:
        logger.info(f"Processando crossmatch: {args.sync_aplic}")
        df_cross = pd.read_excel(args.sync_aplic, sheet_name="Resultados", dtype=str)
        resultado = sincronizar_crossmatch(df_cross)
        print(f"\nFirestore atualizado com crossmatch:")
        print(f"  Movidos para ambos:      {resultado['movidos_para_ambos']}")
        print(f"  Inseridos apenas_aplic:  {resultado['inseridos_apenas_aplic']}")
        raise SystemExit(0)

    # Modo D-1: coleta PNCP e insere em apenas_pncp
    if args.date:
        data_alvo = args.date
    else:
        data_alvo = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

    logger.info(f"Data alvo: {data_alvo}")

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
    import re
    df = pd.read_excel(xlsx_path, dtype=str)
    df["orgaoEntidade_cnpj"] = df["orgaoEntidade_cnpj"].astype(str).apply(
        lambda x: re.sub(r"\D", "", x)
    )

    resultado = sincronizar(df, data_alvo)
    print(f"\nFirestore atualizado — {data_alvo}")
    print(f"  Inseridos:        {resultado['inseridos']}")
    print(f"  Atualizados:      {resultado['atualizados']}")
    print(f"  Alertas ativados: {resultado['alertas']}")

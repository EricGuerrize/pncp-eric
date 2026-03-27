"""
firebase_sync.py — Sincroniza registros PNCP do dia anterior com o Firestore.

Uso standalone:
    python firebase_sync.py                        # D-1 automático
    python firebase_sync.py --date 20260325        # data específica

Fluxo:
  1. Roda o pipeline PNCP para D-1 (ou data fornecida)
  2. Filtra registros do município configurado (CNPJs no DE_PARA)
  3. Para cada registro novo → insere no Firestore
  4. Para registros já existentes → atualiza apenas campos PNCP (não sobrescreve APLIC)
  5. Marca alertaAtivo=True nos registros com prazo vencido e APLIC ainda pendente

Firestore — coleção: licitacoes/{municipio}/{numeroControlePNCP}
    municipio, orgao, modalidade, numero, ano,
    objeto, valor,
    dataPNCP, prazoAplic,
    statusPNCP:  "S"
    statusAPLIC: "pendente"   ← atualizado manualmente / futura integração Oracle
    alertaAtivo: False → True quando prazoAplic < hoje e statusAPLIC == "pendente"
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

CREDENTIALS_PATH = Path(__file__).resolve().parent.parent / "firebase_credentials.json"
COLECAO_RAIZ     = "licitacoes"

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
# Sincronização
# ---------------------------------------------------------------------------

def _doc_de_row(row: pd.Series) -> dict:
    """Converte uma linha do DataFrame PNCP em dicionário Firestore."""
    from google.cloud.firestore_v1 import SERVER_TIMESTAMP

    data_pncp_raw = row.get("dataPublicacaoPncp") or row.get("dataAberturaProposta")
    try:
        data_pncp = pd.to_datetime(data_pncp_raw, errors="coerce")
        data_pncp = data_pncp.to_pydatetime() if pd.notna(data_pncp) else datetime.utcnow()
        # Remove timezone para comparação simples
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
        "municipio":   MUNICIPIO_NOME,
        "orgao":       str(row.get("unidadeOrgao_nomeUnidade") or "")[:80],
        "modalidade":  str(row.get("modalidadeNome") or "")[:60],
        "numero":      str(row.get("numeroCompra") or ""),
        "ano":         str(row.get("anoCompra") or ""),
        "objeto":      str(row.get("objetoCompra") or "")[:300],
        "valor":       valor,
        "cnpj":        str(row.get("orgaoEntidade_cnpj") or ""),
        "dataPNCP":    data_pncp,
        "prazoAplic":  prazo_aplic,
        "statusPNCP":  "S",
        "atualizadoEm": SERVER_TIMESTAMP,
    }


def sincronizar(df_pncp: pd.DataFrame, data_ref: str) -> dict:
    """
    Sincroniza registros do dia com o Firestore.

    Retorna dict com contadores: inseridos, atualizados, alertas_ativados.
    """
    from google.cloud.firestore_v1 import SERVER_TIMESTAMP

    db = _inicializar_firebase()
    colecao = db.collection(COLECAO_RAIZ).document(MUNICIPIO_NOME).collection(data_ref)

    # Filtra apenas registros do município
    mask = df_pncp["orgaoEntidade_cnpj"].astype(str).isin(CNPJS_MUNICIPIO)
    df_municipio = df_pncp[mask].copy()

    logger.info(f"[Firebase] {len(df_municipio)} registros de {MUNICIPIO_NOME} para sincronizar")

    inseridos = atualizados = alertas = 0
    hoje = datetime.utcnow()

    for _, row in df_municipio.iterrows():
        doc_id = str(row.get("numeroControlePNCP") or "").strip().replace("/", "_")
        if not doc_id:
            continue

        ref  = colecao.document(doc_id)
        snap = ref.get()
        doc  = _doc_de_row(row)

        if not snap.exists:
            # Novo registro — insere com statusAPLIC pendente
            doc["statusAPLIC"]  = "pendente"
            doc["alertaAtivo"]  = False
            doc["criadoEm"]     = SERVER_TIMESTAMP
            ref.set(doc)
            inseridos += 1
            logger.info(f"  [+] {doc['orgao']} | {doc['numero']} | {doc['objeto'][:60]}")

        else:
            # Já existe — atualiza apenas campos PNCP, preserva statusAPLIC e alertas
            dados_existentes = snap.to_dict()
            status_aplic = dados_existentes.get("statusAPLIC", "pendente")

            # Verifica se prazo venceu e APLIC ainda pendente → alerta
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
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    parser = argparse.ArgumentParser(description="Sincroniza PNCP D-1 com Firebase")
    parser.add_argument("--date", metavar="YYYYMMDD",
                        help="Data a processar (padrão: ontem)")
    args = parser.parse_args()

    if args.date:
        data_alvo = args.date
    else:
        data_alvo = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

    logger.info(f"Data alvo: {data_alvo}")

    # Verifica se já existe Excel do dia; se não, roda o pipeline
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

    # Remove formatação de CNPJ para comparação
    import re
    df["orgaoEntidade_cnpj"] = df["orgaoEntidade_cnpj"].astype(str).apply(
        lambda x: re.sub(r"\D", "", x)
    )

    resultado = sincronizar(df, data_alvo)
    print(f"\nFirestore atualizado — {data_alvo}")
    print(f"  Inseridos:        {resultado['inseridos']}")
    print(f"  Atualizados:      {resultado['atualizados']}")
    print(f"  Alertas ativados: {resultado['alertas']}")

"""
limpar_firebase.py — Remove do Firestore todos os municípios fora da lista alvo.

Uso:
    python limpar_firebase.py           # dry-run (mostra o que seria removido)
    python limpar_firebase.py --executar # apaga de verdade
"""

import argparse
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, str(Path(__file__).parent))

from firebase_sync import _inicializar_firebase

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
logger = logging.getLogger(__name__)

MUNICIPIOS_ALVO = {"sinop", "lucas_do_rio_verde", "rondolandia", "acorizal", "jangada"}


def _deletar_colecao(col_ref, batch, contador):
    """Deleta todos os docs de uma subcoleção, retorna novo contador."""
    for doc in col_ref.stream():
        batch.delete(doc.reference)
        contador += 1
        if contador % 400 == 0:
            batch.commit()
            batch = col_ref._client.batch()
    return batch, contador


def limpar(dry_run: bool = True):
    db = _inicializar_firebase()
    municipios_col = db.collection("municipios")

    remover = []
    manter = []

    for doc in municipios_col.stream():
        slug = doc.id
        if slug in MUNICIPIOS_ALVO:
            manter.append(slug)
        else:
            remover.append(slug)

    logger.info(f"Municípios para MANTER ({len(manter)}): {sorted(manter)}")
    logger.info(f"Municípios para REMOVER ({len(remover)}): {sorted(remover)}")

    if dry_run:
        logger.info("Modo dry-run — nenhuma alteração feita. Use --executar para apagar.")
        return

    SUBCOLS = ["apenas_pncp", "apenas_aplic", "ambos", "aplic_raw"]
    total_docs = 0

    for slug in remover:
        doc_ref = municipios_col.document(slug)
        batch = db.batch()
        count = 0

        for sub in SUBCOLS:
            batch, count = _deletar_colecao(doc_ref.collection(sub), batch, count)

        batch.delete(doc_ref)
        count += 1
        batch.commit()
        total_docs += count
        logger.info(f"  Removido: {slug} ({count} documentos)")

    logger.info(f"Limpeza concluída. {len(remover)} município(s) removido(s), {total_docs} documentos apagados.")


def main():
    parser = argparse.ArgumentParser(description="Remove municípios fora da lista alvo do Firestore.")
    parser.add_argument("--executar", action="store_true", help="Executa a remoção (sem esta flag, apenas dry-run)")
    args = parser.parse_args()
    limpar(dry_run=not args.executar)


if __name__ == "__main__":
    main()

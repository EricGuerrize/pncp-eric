import pandas as pd
import logging
import config

logger = logging.getLogger(__name__)

def export_to_excel(df: pd.DataFrame, file_date: str):
    """
    Exports the DataFrame to an Excel file using openpyxl.
    """
    filename = f"pncp_contratacoes_{config.UF}_{file_date}.xlsx"
    filepath = config.OUTPUT_DIR / filename
    
    logger.info(f"Exportando {len(df)} registros para {filepath}...")
    
    try:
        df.to_excel(filepath, sheet_name="Contratações", index=False, engine='openpyxl')
        logger.info(f"Exportação concluída com sucesso: {filepath}")
        return filepath
    except Exception as e:
        logger.error(f"Erro ao exportar para Excel: {e}")
        raise

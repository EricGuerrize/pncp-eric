import re
import pandas as pd
import logging
import config

logger = logging.getLogger(__name__)

_ILLEGAL_CHARS_RE = re.compile(r'[\x00-\x08\x0b-\x0c\x0e-\x1f]')

def _sanitize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Remove illegal Excel characters from string columns."""
    df = df.copy()
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].apply(
            lambda v: _ILLEGAL_CHARS_RE.sub(' ', str(v)) if pd.notna(v) else v
        )
    return df

def export_to_excel(df: pd.DataFrame, file_date: str):
    """
    Exports the DataFrame to an Excel file using openpyxl.
    """
    filename = f"pncp_contratacoes_{config.UF}_{file_date}.xlsx"
    filepath = config.OUTPUT_DIR / filename

    logger.info(f"Exportando {len(df)} registros para {filepath}...")

    try:
        df = _sanitize_df(df)
        df.to_excel(filepath, sheet_name="Contratações", index=False, engine='openpyxl')
        logger.info(f"Exportação concluída com sucesso: {filepath}")
        return filepath
    except Exception as e:
        logger.error(f"Erro ao exportar para Excel: {e}")
        raise

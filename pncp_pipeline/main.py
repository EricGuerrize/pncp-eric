import asyncio
import logging
from datetime import datetime, timedelta
import time
import config
from collector import collect_all_data
from normalizer import normalize_results
from dataset_builder import build_dataset, clean_dataset
from excel_exporter import export_to_excel

# Setup basic logging config here so it runs when main is invoked
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(config.LOGS_DIR / "pncp_pipeline.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

async def run_pipeline(target_date: str = None):
    """
    Executa o pipeline completo para a data alvo.
    Se target_date não for fornecido, utiliza D-1 (ontem).
    Formato da data: YYYYMMDD.
    """
    start_time = time.time()
    logger.info("="*50)
    logger.info("INICIANDO PIPELINE DE COLETA PNCP")
    
    # Define target dates
    if not target_date:
        yesterday = datetime.now() - timedelta(days=1)
        target_date = yesterday.strftime("%Y%m%d")
        
    logger.info(f"Data consultada: {target_date} a {target_date}")
    
    try:
        # Step 1: Collect
        raw_results = await collect_all_data(data_inicial=target_date, data_final=target_date)
        
        # Step 2: Normalize
        exec_datetime = datetime.now()
        normalized_data = normalize_results(raw_results, exec_datetime)
        
        # Step 3: Build Dataset
        df = build_dataset(normalized_data)
        
        # Step 4: Clean Dataset
        df = clean_dataset(df)
        
        # Step 5: Export to Excel
        if not df.empty:
            export_to_excel(df, target_date)
            logger.info(f"Pipeline concluído com sucesso. {len(df)} registros processados.")
        else:
            logger.info("Pipeline concluído. Nenhum registro encontrado para esta data.")
            
    except Exception as e:
        logger.error(f"Erro na execução do pipeline: {e}", exc_info=True)
    finally:
        end_time = time.time()
        elapsed = end_time - start_time
        logger.info(f"Tempo total de execução: {elapsed:.2f} segundos")
        logger.info("="*50)

def main():
    asyncio.run(run_pipeline())

if __name__ == "__main__":
    main()

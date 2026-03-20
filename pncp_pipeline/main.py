import asyncio
import argparse
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

async def run_pipeline(data_inicial: str = None, data_final: str = None):
    """
    Executa o pipeline completo para o range de datas.
    Se não fornecido, utiliza D-1 (ontem) para ambas.
    Formato das datas: YYYYMMDD.
    """
    start_time = time.time()
    logger.info("="*50)
    logger.info("INICIANDO PIPELINE DE COLETA PNCP")

    # Define target dates
    if not data_inicial:
        yesterday = datetime.now() - timedelta(days=1)
        data_inicial = yesterday.strftime("%Y%m%d")
    if not data_final:
        data_final = data_inicial

    file_date = data_inicial if data_inicial == data_final else f"{data_inicial}_{data_final}"
    logger.info(f"Data consultada: {data_inicial} a {data_final}")

    try:
        # Step 1: Collect
        raw_results = await collect_all_data(data_inicial=data_inicial, data_final=data_final)

        # Step 2: Normalize
        exec_datetime = datetime.now()
        normalized_data = normalize_results(raw_results, exec_datetime)

        # Step 3: Build Dataset
        df = build_dataset(normalized_data)

        # Step 4: Clean Dataset
        df = clean_dataset(df)

        # Step 5: Export to Excel
        if not df.empty:
            export_to_excel(df, file_date)
            logger.info(f"Pipeline concluído com sucesso. {len(df)} registros processados.")
        else:
            logger.info("Pipeline concluído. Nenhum registro encontrado para este período.")

    except Exception as e:
        logger.error(f"Erro na execução do pipeline: {e}", exc_info=True)
    finally:
        end_time = time.time()
        elapsed = end_time - start_time
        logger.info(f"Tempo total de execução: {elapsed:.2f} segundos")
        logger.info("="*50)

def main():
    parser = argparse.ArgumentParser(description="Pipeline de coleta PNCP")
    parser.add_argument("--from", dest="data_inicial", metavar="YYYYMMDD",
                        help="Data inicial (padrão: D-1)")
    parser.add_argument("--to", dest="data_final", metavar="YYYYMMDD",
                        help="Data final (padrão: igual a --from)")
    args = parser.parse_args()
    asyncio.run(run_pipeline(data_inicial=args.data_inicial, data_final=args.data_final))

if __name__ == "__main__":
    main()

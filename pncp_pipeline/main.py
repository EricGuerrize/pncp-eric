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
            
            # Step 6 & 7: Crossmatch Automático e Sincronização Firebase
            try:
                from firebase_sync import sincronizar, sincronizar_crossmatch
                from crossmatch import crossmatch, carregar_aplic
                
                # O Firebase_sync tentará puxar config.CREDENTIALS_PATH
                # Passo 7A: Sincroniza dados normais PNCP primeiro
                logger.info("=== STEP 7: Sincronizando PNCP (Base) no Firebase ===")
                sincronizar(df, data_inicial)
                
                # Passo 6: Verifica se há arquivos APLIC para cruzar
                csv_files = list(config.INPUT_DIR.glob("*.csv")) + list(config.INPUT_DIR.glob("*.CSV"))
                for aplic_file in csv_files:
                    logger.info(f"=== STEP 6: Cruzamento automático PNCP x APLIC ({aplic_file.name}) ===")
                    df_aplic = carregar_aplic(aplic_file)
                    df_cross, _ = crossmatch(df, df_aplic)
                    
                    if not df_cross.empty:
                        # Inferir municipio pela string no nome do arquivo, ex: licitacao_lrv_2026 -> lucas_do_rio_verde
                        name_lower = aplic_file.name.lower()
                        mun = "sinop"
                        if "lrv" in name_lower or "lucas" in name_lower: mun = "lucas_do_rio_verde"
                        elif "cuiaba" in name_lower: mun = "cuiaba"
                        elif "sinop" in name_lower: mun = "sinop"
                        
                        logger.info(f"=== STEP 7B: Sincronizando resultados do Cruzamento ({mun}) no Firebase ===")
                        sincronizar_crossmatch(df_cross, municipio=mun)
                    else:
                        logger.info(f"Nenhum resultado gerado para o cruzamento de {aplic_file.name}")
                        
            except Exception as ex:
                logger.error(f"Erro nas etapas adicionais (Step 6/7): {ex}", exc_info=True)
                
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

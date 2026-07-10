import schedule
import time
import logging
from main import main

logger = logging.getLogger(__name__)

def job():
    logger.info("Iniciando job agendado diário...")
    main()

if __name__ == "__main__":
    # Configurar execução para todos os dias à 01:00 da manhã
    schedule.every().day.at("01:00").do(job)
    
    print("Scheduler iniciado. Aguardando horário programado (01:00)...")
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        print("Scheduler encerrado pelo usuário.")

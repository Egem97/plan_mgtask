import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from utils.utils import *
from functions.agricola import *
from utils.get_token import get_access_token
from functions.load_onedrive import *
from functions.tipo_cambio import tipo_cambio_load_data
from functions.proc_files_xlsx import pipeline_agritracer
from functions.hubcrop import pipeline_hubcrop
async def main():
    scheduler = AsyncIOScheduler()
    
    scheduler.add_job(cosecha_load_data, 'interval', minutes=15)
    #scheduler.add_job(fertiriego_load_data, 'interval', minutes=45)
    scheduler.add_job(tipo_cambio_load_data, 'cron', hour=7, minute=0, timezone='America/Lima')
    scheduler.add_job(pipeline_hubcrop, 'cron', hour='9,15', minute=0, timezone='America/Lima')
    scheduler.add_job(pipeline_agritracer, 'cron', hour='8,14', minute=30, timezone='America/Lima', 
                      max_instances=3, misfire_grace_time=3600)
    
    scheduler.start()
    print("Scheduler iniciado. Ejecutando jobs.")

    # Mantener el proceso vivo
    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == "__main__":
    asyncio.run(main())
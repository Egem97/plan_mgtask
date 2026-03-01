import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from utils.utils import *
from functions.agricola import *
from utils.get_token import get_access_token
from functions.load_onedrive import *
from functions.tipo_cambio import tipo_cambio_load_data
from functions.proc_files_xlsx import pipeline_agritracer
from functions.hubcrop import pipeline_hubcrop
from functions.estacion_meteorologica import pipeline_meteorologia

async def pipeline_agritracer_job():
    for attempt in range(1, 6):
        try:
            print(f"--- Ejecutando pipeline_agritracer: Intento {attempt}/5 ---")
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(None, pipeline_agritracer)
            if result:
                print(f"✅ pipeline_agritracer completado con éxito en el intento {attempt}")
                return
            else:
                print(f"⚠️ pipeline_agritracer no retornó éxito en el intento {attempt}")
        except Exception as e:
            print(f"❌ Error en el intento {attempt} de pipeline_agritracer: {str(e)}")
        
        if attempt < 5:
            wait_time = 30  # Esperar 30 segundos entre reintentos
            print(f"🔄 Reintentando en {wait_time} segundos...")
            await asyncio.sleep(wait_time)
            
    print("❌ pipeline_agritracer falló después de 5 intentos.")

async def main():
    scheduler = AsyncIOScheduler()
    
    scheduler.add_job(cosecha_load_data, 'cron', hour=10, timezone='America/Lima')
    scheduler.add_job(load_kissflow_fertirriego, 'interval', minutes=8)
    scheduler.add_job(load_biometria_2026, 'interval', minutes=5)
    scheduler.add_job(tipo_cambio_load_data, 'cron', hour=7, minute=0, timezone='America/Lima')
    scheduler.add_job(pipeline_hubcrop, 'cron', hour='9,15,17', minute=0, timezone='America/Lima')
    scheduler.add_job(pipeline_agritracer_job, 'cron', hour='8-23', minute=25, timezone='America/Lima')
    scheduler.add_job(pipeline_meteorologia, 'cron', hour='8,20', minute=0, timezone='America/Lima')
    
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
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
from functions.costos import *
from functions.net_pipeline import pipeline_netsuite_ordenes
from functions.mayor_analitico_pipeline import (
    incremental as mayor_analitico_incremental,
    full_load as mayor_analitico_full_load,
)
from functions.kissflow import upload_datakiss_bd_formulario_camaras
from functions.comex import update_comex

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
    #scheduler.add_job(PLT_ACTIVIDADES_GENERAL, 'cron', hour='7-20', minute='27', timezone='America/Lima')
    scheduler.add_job(update_comex, 'cron', hour='8-20', minute='15,45', timezone='America/Lima')
    scheduler.add_job(cosecha_load_data, 'cron', hour='7-20', minute='28,55', timezone='America/Lima')
    scheduler.add_job(load_kissflow_fertirriego, 'interval', minutes=14)
    scheduler.add_job(load_biometria_2026,'cron', hour='7-20', minute='28,55', timezone='America/Lima')
    scheduler.add_job(load_biometria_experimental_2026,'cron', hour='7-20', minute='28,55', timezone='America/Lima')
    scheduler.add_job(tipo_cambio_load_data, 'cron', hour=7, minute=0, timezone='America/Lima')
    scheduler.add_job(pipeline_hubcrop, 'cron', hour='9,15,17', minute=0, timezone='America/Lima')
    scheduler.add_job(pipeline_agritracer_job, 'cron', hour='8-23', minute=20, timezone='America/Lima')
    scheduler.add_job(upload_datakiss_bd_formulario_camaras, 'cron', hour='8-23', minute=20, timezone='America/Lima')
    
    scheduler.add_job(pipeline_meteorologia, 'cron', hour='8,20', minute=0, timezone='America/Lima')
    scheduler.add_job(plt_load_data, 'cron', hour='8-23', minute=20, timezone='America/Lima')
    scheduler.add_job(proy_2026_load_data, 'interval', minutes=60)
    scheduler.add_job(load_proyecciones_2026, 'cron', hour='7-20', minute='28,55', timezone='America/Lima')
    ##COSTOS
    scheduler.add_job(load_costo_laboral_gh, 'cron', hour='8,20', minute=0, timezone='America/Lima')
    scheduler.add_job(
        pipeline_netsuite_ordenes,
        'cron',
        day_of_week='mon-fri',
        hour='8-19',
        minute='13',
        timezone='America/Lima'
    )
    """
    # Mayor analítico (CD_pC) cada 30 min + recarga completa semanal (red de seguridad)
    scheduler.add_job(
        mayor_analitico_incremental,
        'cron',
        day_of_week='mon-fri',
        hour='8-19',
        minute='0,30',
        timezone='America/Lima',
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        mayor_analitico_full_load,
        'cron',
        day_of_week='sun',
        hour=3,
        minute=15,
        timezone='America/Lima',
        max_instances=1,
        coalesce=True,
    )
    """
    #load_proyecciones_2026()
    scheduler.start()
    print("Scheduler iniciado. Ejecutando jobs.")

    # Mantener el proceso vivo
    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == "__main__":
    # Evita UnicodeEncodeError al imprimir emojis (✅/❌) en consolas cp1252 (Windows)
    import sys
    for _stream in (sys.stdout, sys.stderr):
        try:
            _stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass
    asyncio.run(main())
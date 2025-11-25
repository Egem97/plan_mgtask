import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from utils.utils import *
from functions.agricola import *
from utils.get_token import get_access_token
from functions.load_onedrive import *



async def main():
    scheduler = AsyncIOScheduler()
    
    scheduler.add_job(cosecha_load_data, 'interval', hours=1)
    scheduler.add_job(fertiriego_load_data, 'interval', minutes=85)
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
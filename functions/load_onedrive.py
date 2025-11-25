import pandas as pd
import logging
from utils.get_api import subir_archivo_con_reintento
from utils.get_token import get_access_token
from functions.agricola import *



def cosecha_load_data():
    print(f"📤 Subiendo archivo 'COSECHA' a OneDrive...")
    COSECHA_DF= data_cosecha()
    access_token = get_access_token()
    resultado = subir_archivo_con_reintento(
        access_token=access_token,
        dataframe=COSECHA_DF,
        nombre_archivo="COSECHA CAMPO.parquet",
        drive_id="b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR",
        folder_id="01XOBWFSDI34HN5SD7HBHZRUBPX3457IPQ",
        type_file="parquet"
    )
    if resultado:
        print(f"✅ Proceso completado exitosamente")
        return True
    else:
        print(f"❌ Error al subir el archivo")
        return False

def fertiriego_load_data():
    print(f"📤 Subiendo archivos 'FERITIRIEGO' a OneDrive...")
    
    resultado_1 = subir_archivo_con_reintento(
        access_token=get_access_token(),
        dataframe=aplicativoNutricional(),
        nombre_archivo="APLICACIONES NUTRICIONALES.parquet",
        drive_id="b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR",
        folder_id="01XOBWFSDI34HN5SD7HBHZRUBPX3457IPQ",
        type_file="parquet"
    )

    resultado_2 = subir_archivo_con_reintento(
        access_token=get_access_token(),
        dataframe=FertiRiego(),
        nombre_archivo="INSUMOS.parquet",
        drive_id="b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR",
        folder_id="01XOBWFSDI34HN5SD7HBHZRUBPX3457IPQ",
        type_file="parquet"
    )

    resultado_3 = subir_archivo_con_reintento(
        access_token=get_access_token(),
        dataframe=parametros_campo(),
        nombre_archivo="MUESTRAS.parquet",
        drive_id="b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR",
        folder_id="01XOBWFSDI34HN5SD7HBHZRUBPX3457IPQ",
        type_file="parquet"
    )

    resultado_4 = subir_archivo_con_reintento(
        access_token=get_access_token(),
        dataframe=drenaje_campo(),
        nombre_archivo="DRENAJE.parquet",
        drive_id="b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR",
        folder_id="01XOBWFSDI34HN5SD7HBHZRUBPX3457IPQ",
        type_file="parquet"
    )

    if resultado_4:
        print(f"✅ Proceso completado exitosamente")
        return True
    else:
        print(f"❌ Error al subir el archivo")
        return False
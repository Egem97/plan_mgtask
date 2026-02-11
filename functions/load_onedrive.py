import pandas as pd
import logging
from utils.get_api import subir_archivo_con_reintento
from utils.get_token import get_access_token
from functions.agricola import *
from functions.recursos_humanos import read_costo_laboral
from functions.ma import read_ma



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




def kg_campo_load():
    access_token = get_access_token()
    print(f"📤 Subiendo archivo 'KG CAMPO' a OneDrive...")
    kg_campo= clean_cosecha_2()
    resultado = subir_archivo_con_reintento(
        access_token=access_token,
        dataframe=kg_campo,
        nombre_archivo="KG CAMPO.parquet",
        drive_id="b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR",
        folder_id="01XOBWFSAIDPG5FEZ5AJGK24FSIZY5YRUD",
        type_file="parquet"
    )
    if resultado:
        print(f"✅ Proceso completado exitosamente")
        return True
    else:
        print(f"❌ Error al subir el archivo")
        return False




def costo_laboral_diario_load():
    access_token = get_access_token()
    print(f"📤 Subiendo archivo 'COSTO LABORAL DIARIO' a OneDrive...")
    costo_laboral_diario= read_costo_laboral()
    resultado = subir_archivo_con_reintento(
        access_token=access_token,
        dataframe=costo_laboral_diario,
        nombre_archivo="COSTO LABORAL DIARIO.parquet",
        drive_id="b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR",
        #folder_id="01XOBWFSDI34HN5SD7HBHZRUBPX3457IPQ",
        folder_id="01XOBWFSAIDPG5FEZ5AJGK24FSIZY5YRUD",
        type_file="parquet"
    )
    if resultado:
        print(f"✅ Proceso completado exitosamente")
        return True
    else:
        print(f"❌ Error al subir el archivo")
        return False


def ma_load_data():
    access_token = get_access_token()
    print(f"📤 Subiendo archivo 'MAYOR ANALITICO' a OneDrive...")
    ma= read_ma()
    resultado = subir_archivo_con_reintento(
        access_token=access_token,
        dataframe=ma,
        nombre_archivo="MAYOR ANALITICO.parquet",
        drive_id="b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR",
        #folder_id="01XOBWFSDI34HN5SD7HBHZRUBPX3457IPQ",
        folder_id="01XOBWFSAIDPG5FEZ5AJGK24FSIZY5YRUD",
        type_file="parquet"
    )
    if resultado:
        print(f"✅ Proceso completado exitosamente")
        return True
    else:
        print(f"❌ Error al subir el archivo")
        return False


def meq_load_data():
    access_token = get_access_token()
    print(f"📤 Subiendo archivo 'MILIEQUIVALENTES' a OneDrive...")
    meq= transform_kissflow_meq()
    resultado = subir_archivo_con_reintento(
        access_token=access_token,
        dataframe=meq,
        nombre_archivo="MILIEQUIVALENTES.parquet",
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
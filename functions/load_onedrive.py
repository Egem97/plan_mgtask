import pandas as pd
import logging
from utils.get_api import subir_archivo_con_reintento
from utils.get_token import get_access_token
from functions.agricola import *

from functions.biometria import pipeline_biometria



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

############################################################################################################

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

def load_kissflow_muestras():
    print(f"📤 Subiendo archivos 'MUESTRAS' a OneDrive...")
    
    upload = subir_archivo_con_reintento(
        access_token=get_access_token(),
        dataframe=completed_kissflow_muestras(),
        nombre_archivo="MUESTRAS.parquet",
        drive_id="b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR",
        folder_id="01XOBWFSDI34HN5SD7HBHZRUBPX3457IPQ",
        type_file="parquet"
    )
    if upload:
        print(f"✅ Proceso completado exitosamente")
        return True
    else:
        print(f"❌ Error al subir el archivo")
        return False

def load_kissflow_drenajes():
    print(f"📤 Subiendo archivos 'DRENAJE' a OneDrive...")
    
    upload = subir_archivo_con_reintento(
        access_token=get_access_token(),
        dataframe=transform_kissflow_drenajes(),
        nombre_archivo="DRENAJE.parquet",
        drive_id="b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR",
        folder_id="01XOBWFSDI34HN5SD7HBHZRUBPX3457IPQ",
        type_file="parquet"
    )
    if upload:
        print(f"✅ Proceso completado exitosamente")
        return True
    else:
        print(f"❌ Error al subir el archivo")
        return False


def load_kissflow_apl_nutricionales():
    print(f"📤 Subiendo archivos 'APLICACIONES NUTRICIONALES' a OneDrive...")
    
    upload = subir_archivo_con_reintento(
        access_token=get_access_token(),
        dataframe=transform_kissflow_nutricionales(),
        nombre_archivo="APLICACIONES NUTRICIONALES.parquet",
        drive_id="b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR",
        folder_id="01XOBWFSDI34HN5SD7HBHZRUBPX3457IPQ",
        type_file="parquet"
    )
    if upload:
        print(f"✅ Proceso completado exitosamente")
        return True
    else:
        print(f"❌ Error al subir el archivo")
        return False


def load_kissflow_fertirriego():
    load_kissflow_muestras()
    load_kissflow_drenajes()
    load_kissflow_apl_nutricionales()
    meq_load_data()
    print("FINALIZADO FERTIRRIEGO")


def load_biometria_2026():
    print(f"📤 Subiendo archivos 'BIOMETRIA 2026' a OneDrive...")
    
    upload = subir_archivo_con_reintento(
        access_token=get_access_token(),
        dataframe=pipeline_biometria(),
        nombre_archivo="BIOMETRIA_2026.parquet",
        drive_id="b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR",
        folder_id="01XOBWFSEADJMTLAK7QNE3PNMDWQT2ZLF2",
        type_file="parquet"
    )
    if upload:
        print(f"✅ Proceso completado exitosamente")
        return True
    else:
        print(f"❌ Error al subir el archivo")
        return False
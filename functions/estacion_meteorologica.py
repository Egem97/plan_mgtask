from utils.get_token import get_access_token
from utils.get_api import listar_archivos_en_carpeta_compartida,subir_archivo_con_reintento,get_tc_sunat_diario

from utils.helpers import get_download_url_by_name
from utils.utils import *

from functions.agricola import cosecha_datasets,clean_cosecha_2
import pandas as pd
import os
import streamlit as st
import numpy as np
from datetime import datetime
from utils.get_meteo import InnovaWeatherAPI
from utils.get_token import get_access_token


def pipeline_meteorologia():
    drive_id = "b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR"
    folder_id = "01XOBWFSDBKVECP3XJCVDKF6KP6TJYVTQO"
    data = listar_archivos_en_carpeta_compartida(
        get_access_token(),
        drive_id,
        folder_id
    )
    url_excel_1 = get_download_url_by_name(data, "ESTACIONES_METEOROLOGICAS_20250101_20260125.parquet")
    hdf = pd.read_parquet(url_excel_1)
    api_client = InnovaWeatherAPI()

    api_client.login()

    df = api_client.get_all_stations_data()
    df = pd.concat([hdf,df],axis=0)
    resultado = subir_archivo_con_reintento(
        access_token=get_access_token(),
        dataframe=df,
        nombre_archivo="ESTACIONES_METEOROLOGICAS.parquet",
        drive_id=drive_id,
        folder_id=folder_id,
        type_file="parquet"
    )
    if resultado:
        print(f"✅ Proceso completado exitosamente")
        return True
    else:
        print(f"❌ Error al subir el archivo")
        return False
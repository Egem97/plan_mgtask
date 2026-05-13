from utils.get_token import get_access_token
from utils.get_api import listar_archivos_en_carpeta_compartida,subir_archivo_con_reintento,get_tc_sunat_diario
from functions.proc_files_xlsx import agritacer_data_detalle,agri_xlsx_data
from utils.helpers import get_download_url_by_name
from utils.utils import *
import pandas as pd
import os
import streamlit as st
import numpy as np
from datetime import datetime
from functions.load_onedrive import *
from functions.biometria import *
from functions.estacion_meteorologica import pipeline_meteorologia
from utils.get_meteo import InnovaWeatherAPI
from functions.costos import plt_load_data
from utils.get_kiss import fetch_all_kissflow


st.set_page_config(page_title="web pruebas", page_icon=":tada:")
st.title("pruebas")
with st.spinner("Wait for it...", show_time=True):
    load_proyecciones_2026()
st.success("ww")
#df = proyecciones_2026()
#st.dataframe(df)
#st.success("owo")
#meq_df = transform_kissflow_meq()
#dff = fetch_all_kissflow("COP01_BD_PLANES_DE_TRABAJO")
#st.dataframe(dff)
#api_client = InnovaWeatherAPI()
#api_client.login()
#df = api_client.get_all_stations_data()
#st.write(df.shape)
#st.dataframe(df)
#plt_load_data()
#st.success("ssss")

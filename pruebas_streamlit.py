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
from functions.costos import *
from utils.get_kiss import fetch_all_kissflow


st.set_page_config(page_title="web pruebas", page_icon=":tada:")
st.title("pruebas")
from functions.transporte import *
with st.spinner("Wait for it...", show_time=True):
    load_biometria_experimental_2026()
st.success("ww")

#df = pipeline_biometria_experimental()
#st.dataframe(df)
#proy_df,ppt_df = proy_2026()
#st.dataframe(ppt_df)
#proy_df,ppt_df = proy_2026()s
#ppt_df["CAMPAÑA"] = "Campañsa 2026"
#ppt_df["Semana Año"] = ppt_df["SEMANA"].astype(str) + "-" + ppt_df["SEMANA"].apply(lambda x: "2027" if x < 23 else "2026")

#st.write(ppt_df.shape)
#st.dataframe(ppt_df)


#from functions.costos import PLT_CORE_
#df,dff,index_df = PLT_CORE_()
#st.write(df.shape)
#st.dataframe(df)
#proy_df,ppt_df = proy_2026()
#st.dataframe(proy_df)
#st.dataframe(ppt_df)

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

#cosecha_datasets


from functions.costos import *
from functions.transporte import *
#df,kias_df = transform_camaras_kias()
#df,transp_df = costos_cosecha_2026()

#st.dataframe(df)
#st.dataframe(kias_df)
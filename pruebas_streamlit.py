from utils.get_token import get_access_token
from utils.get_api import listar_archivos_en_carpeta_compartida,subir_archivo_con_reintento,get_tc_sunat_diario
from functions.proc_files_xlsx import agritacer_data_detalle,agri_xlsx_data
from utils.helpers import get_download_url_by_name
from utils.utils import *
from functions.recursos_humanos import read_costo_laboral

import pandas as pd
import os
import streamlit as st
import numpy as np
from datetime import datetime



st.set_page_config(page_title="web pruebas", page_icon=":tada:")
st.title("pruebas")



from functions.biometria import *
#load_biometria_2026()
#st.success("CARGADO BIOMETRIA")
dff = pipeline_biometria()
st.write(dff.shape)
st.dataframe(dff)
#if st.button("SAVE"):
#    dff.to_parquet(r"C:\Users\EdwardoGiampiereEnri\OneDrive - ALZA PERU GROUP S.A.C\DATASETS_BI\GENERAL DATA\BIOMETRIA\BIOMETRIA_2026.parquet",index=False)



from utils.get_token import get_access_token
from utils.get_api import listar_archivos_en_carpeta_compartida,subir_archivo_con_reintento,get_tc_sunat_diario
from functions.proc_files_xlsx import agritacer_data_detalle,agri_xlsx_data
from utils.helpers import get_download_url_by_name
from utils.utils import *
from functions.recursos_humanos import read_costo_laboral
from functions.load_onedrive import costo_laboral_diario_load,kg_campo_load,ma_load_data
from functions.agricola import cosecha_datasets,clean_cosecha_2
import pandas as pd
import os
import streamlit as st
import numpy as np
from datetime import datetime

from functions.tipo_cambio import tipo_cambio_load_data,tipo_cambio_extract
from functions.ma import read_ma

st.set_page_config(page_title="web pruebas", page_icon=":tada:")
st.title("pruebas")

def test1(access_token):
    data = listar_archivos_en_carpeta_compartida(
        access_token,
        "b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR",
        "01XOBWFSAWRK2MNONLCZAIMP753DZ3SXTC"
    )
    url_parquet = get_download_url_by_name(data, "HUBCROP_2025_.parquet")
    #url_excel_2 = get_download_url_by_name(data, "REGISTRO APLICACIONES NUTRICIONALES-FUNDO QBERRIES.xlsx")
    return pd.read_parquet(url_parquet, engine="pyarrow")  #, pd.read_excel(url_excel_2, sheet_name="BD")



#download_files_c1()
df = test1(get_access_token())
st.write(df.shape)
st.dataframe(df)
##test_1 = test1(get_access_token())
#st.write(test_1.shape)
#st.dataframe(test_1)




#from functions.hubcrop import *
#inicio = datetime.now()
#data25_df = pd.read_parquet(r"C:\Users\EdwardoGiampiereEnri\OneDrive - ALZA PERU GROUP S.A.C\DATASETS_BI\GENERAL DATA\HUBCROP\HUBCROP_2025_.parquet")
#st.write(data25_df.shape)
#st.dataframe(data25_df)
#dff = get_hubcrop_cosecha_default()
#df = clean_hubcrop(dff)
#st.write(df.shape)
#st.dataframe(df)
#hubcrop_dff = pd.concat([data25_df,df],axis=0)
#st.write(hubcrop_dff.shape)
#st.dataframe(hubcrop_dff)
#hubcrop_dff.to_parquet(r"C:\Users\EdwardoGiampiereEnri\OneDrive - ALZA PERU GROUP S.A.C\DATASETS_BI\GENERAL DATA\HUBCROP\HUBCROP_GENERAL.parquet",index=False,compression="snappy")
#fin = datetime.now()
#st.write(fin - inicio)











"""
inicio = datetime.now()
df = run_hubcrop_query(query)
fin = datetime.now()
st.write(fin - inicio)
st.write(df.shape)
st.dataframe(df)
st.write(fin - inicio)
df.to_parquet("HUBCROP_2025.parquet",index=False,compression="snappy")
"""
#df.to_parquet("AGRITRACER_2025.parquet",index=False)
#from functions.load_onedrive import cosecha_load_data
#cosecha_load_data()
#st.write("actualizado")
"""
from functions.fundos_cosecha_bi import proy_penta_any,proy_penta_qberry

access_token = get_access_token()
df = proy_penta_any(access_token)
df.columns = [str(c).strip().upper() for c in df.columns]
columns_ = ["FUNDO","VARIEDAD","VARIABLE","SEMANA","KILOS"]
df = df[columns_]
for col in ["FUNDO","VARIEDAD","VARIABLE"]:
    df[col] = df[col].astype(str)
    df[col] = df[col].str.strip()

for col in ["SEMANA","KILOS"]:
    df[col] = df[col].fillna(0)
    df[col] = df[col].astype(float)
    
print(df.columns)
st.write(df.shape)
st.dataframe(df)

dff = proy_penta_qberry(access_token)
dff.columns = [str(c).strip().upper() for c in df.columns]
columns_ = ["FUNDO","VARIEDAD","VARIABLE","SEMANA","KILOS"]
dff = dff[columns_]
for col in ["FUNDO","VARIEDAD","VARIABLE"]:
    dff[col] = dff[col].astype(str)
    dff[col] = dff[col].str.strip()

for col in ["SEMANA","KILOS"]:
    dff[col] = dff[col].fillna(0)
    dff[col] = dff[col].astype(float)
st.write(dff.shape)
st.dataframe(dff)

proyectados_df = pd.concat([df,dff],axis=0)
proyectados_df["AÑO"] = proyectados_df.apply(lambda x: "2025" if x["SEMANA"] >= 24 else "2026", axis=1)
proyectados_df["WEEK YEAR"] = proyectados_df["SEMANA"].astype(int).astype(str) + "-" + proyectados_df["AÑO"].astype(int).astype(str)
proyectados_df["Indicador"] = proyectados_df["FUNDO"] + " " + proyectados_df["VARIABLE"].str.upper()

indicadores_proy = proyectados_df.groupby(["Indicador","FUNDO"]).agg({"KILOS":"sum"}).reset_index()
indicadores_proy = indicadores_proy[["Indicador","FUNDO"]]

st.write(proyectados_df.shape)
st.dataframe(proyectados_df)

proyectados_df = pd.pivot_table(proyectados_df,index=["AÑO","FUNDO","SEMANA","WEEK YEAR"],values="KILOS",columns="Indicador",aggfunc="sum",fill_value=0)
proyectados_df = proyectados_df.reset_index()
st.write(proyectados_df.shape)
st.dataframe(proyectados_df)

st.write(indicadores_proy.shape)
st.dataframe(indicadores_proy)

indicadores_proy.to_parquet("INDICADORES PROYECTADOS.parquet",index=False)
proyectados_df.to_parquet("KILOS MODULOS VARIABLE.parquet",index=False)
"""
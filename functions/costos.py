import streamlit as st
import pandas as pd
from utils.utils import *
from utils.helpers import get_download_url_by_name
from utils.get_api import listar_archivos_en_carpeta_compartida,subir_archivo_con_reintento
from utils.utils import read_excel_fast
from utils.get_token import get_access_token

def planes_trabajo_data():
    data = listar_archivos_en_carpeta_compartida(
        get_access_token(),
        "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        "01KM43WT4OE5STC3Y275A3F3U2KF2OLQCS"
    )
    url_excel_1 = get_download_url_by_name(data, "Registros Kissflow - Planes de trabajo.xlsx")
    actividad = read_excel_fast(url_excel_1, sheet_name="Hoja1")
    insumos = read_excel_fast(url_excel_1, sheet_name="HOJA 2")
    return (actividad,insumos)

def transform_plt():
    actividad_df,insumos_df = planes_trabajo_data()
    actividad_df.columns = (
                actividad_df.columns.astype(str)
                .str.normalize('NFKD')
                .str.encode('ascii', errors='ignore')
                .str.decode('utf-8')
                .str.replace('\n', ' ', regex=False)
                .str.replace('.', '', regex=False)
                .str.strip()
                .str.upper()
    )
    insumos_df.columns = (
                insumos_df.columns.astype(str)
                .str.normalize('NFKD')
                .str.encode('ascii', errors='ignore')
                .str.decode('utf-8')
                .str.replace('\n', ' ', regex=False)
                .str.replace('.', '', regex=False)
                .str.strip()
                .str.upper()
    )
    actividad_df = actividad_df.rename(columns={'SUB-AREA':'SUBAREA','ANO':'YEAR','TOTAL $':'TOTAL'})
    actividad_df["FECHA"] = pd.to_datetime(actividad_df["FECHA"]).dt.date
    for col_num in ["YEAR","SEMANA","FACTOR","JORNALES","TOTAL"]:
        actividad_df[col_num] = actividad_df[col_num].fillna(0)
        #actividad_df[col_num] = actividad_df[col_num].astype(str)
        actividad_df[col_num] = actividad_df[col_num].astype(float)
        
    for col_str in ["FUNDO","AREA","SUBAREA","ACTIVIDAD","TIPO"]:    
        actividad_df[col_str] = actividad_df[col_str].fillna("-")
        actividad_df[col_str] = actividad_df[col_str].astype(str)
        actividad_df[col_str] = actividad_df[col_str].str.strip()
        actividad_df[col_str] = actividad_df[col_str].str.upper()

    actividad_df["FUNDO"] = actividad_df["FUNDO"].fillna("NO ESPECIFICADO")
    actividad_df["FUNDO"] = actividad_df["FUNDO"].replace({
        "GAP": "GAP BERRIES",
        "CANYON BERRIES":"EL POTRERO",
        "LICAPA I":"LICAPA",
        "SAN JOSE I":"SAN JOSE",
        "SAN JOSEII":"SAN JOSE II"
    })
    actividad_df["ACTIVIDAD"] = actividad_df["ACTIVIDAD"].apply(quitar_tildes)
    #actividad_df["ACTIVIDAD"] = actividad_df["ACTIVIDAD"].replace({
        
    #})
    #print(actividad_df["ACTIVIDAD"].unique())
    ################################
    insumos_df["FECHA"] = pd.to_datetime(insumos_df["FECHA"]).dt.date
    insumos_df = insumos_df.rename(columns={'ANO':'YEAR'})

    for col_num in ["YEAR","SEMANA","FACTOR","CANTIDAD","TOTAL"]:
        insumos_df[col_num] = insumos_df[col_num].fillna(0)
        insumos_df[col_num] = insumos_df[col_num].astype(float)
    st.dataframe(insumos_df)
    for col_str in ["FUNDO","INSUMO","VARIEDAD","TIPO"]:    
        insumos_df[col_str] = insumos_df[col_str].fillna("-")
        insumos_df[col_str] = insumos_df[col_str].astype(str)
        insumos_df[col_str] = insumos_df[col_str].str.strip()
        insumos_df[col_str] = insumos_df[col_str].str.upper()
    insumos_df["FUNDO"] = insumos_df["FUNDO"].fillna("NO ESPECIFICADO")
    insumos_df["FUNDO"] = insumos_df["FUNDO"].replace({
        "GAP": "GAP BERRIES",
        "CANYON BERRIES":"EL POTRERO",
        "LICAPA I":"LICAPA",
        "SAN JOSE I":"SAN JOSE",
        "SAN JOSEII":"SAN JOSE II"
    })
    return (actividad_df,insumos_df)

def plt_load_data():
    print(f"📤 Subiendo archivos 'PLANES DE TRABAJO' a OneDrive...")
    actividad_df, insumos_df =transform_plt()
    resultado_1 = subir_archivo_con_reintento(
        access_token=get_access_token(),
        dataframe=actividad_df,
        nombre_archivo="PLT_ACTIVIDADES.parquet",
        drive_id="b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        folder_id="01KM43WT4FS6JNXKKHRNCJZMAXLX56IOEQ",
        type_file="parquet"
    )
    if resultado_1:
        print(f"✅ Proceso 1 completado exitosamente")
    resultado_2 = subir_archivo_con_reintento(
        access_token=get_access_token(),
        dataframe=insumos_df,
        nombre_archivo="PLT_INSUMOS.parquet",
        drive_id="b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        folder_id="01KM43WT4FS6JNXKKHRNCJZMAXLX56IOEQ",
        type_file="parquet"
    )
    if resultado_2:
        print(f"✅ Proceso 2 completado exitosamente")
    else:
        print(f"❌ Error al subir el archivo")
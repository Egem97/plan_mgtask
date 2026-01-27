import pandas as pd
import numpy as np
import os
from utils.utils import quitar_tildes,time_to_decimal_hours,format_lote
from task.download_agritracer import download_files_c1
from utils.get_token import get_access_token
from utils.get_api import listar_archivos_en_carpeta_compartida,subir_archivo_con_reintento,get_tc_sunat_diario
from utils.helpers import get_download_url_by_name


#@st.cache_data(persist=True)
def agritacer_data_detalle(df):
    df = df[df["Fecha"].notna()]
    df['Total Horas'] = df['Total Horas'].apply(time_to_decimal_hours)
    df["Actividad"] = df["Actividad"].apply(quitar_tildes)
    df["Fecha"] = pd.to_datetime(df["Fecha"],format="%d/%m/%Y").dt.date
    df["Documento"] = df["Documento"].fillna(99999999)
    df["Documento"] = df["Documento"].astype(str)
    df["Trabajador"] = df["Trabajador"].fillna("-")
    df["Tipo de Planilla"] = df["Tipo de Planilla"].fillna("NO ESPECIFICADO")
    df["F. Ingreso"] = pd.to_datetime(df["F. Ingreso"],errors="coerce")
    df["F. Cese"] = pd.to_datetime(df["F. Cese"],errors="coerce")
    df["Partida Presupuestaria"] = df["Partida Presupuestaria"].fillna("NO ESPECIFICADO")
    df["Fundo"] = df["Fundo"].fillna("NO ESPECIFICADO")
    df["Fundo"] = df["Fundo"].replace("GAP", "GAP BERRIES")
    df["Módulo"] = df["Módulo"].fillna("-")
    df["Módulo Turno"] = df["Módulo Turno"].fillna("LOTE X")
    df["Variedad"] = df["Variedad"].fillna("NO ESPECIFICADO")
    df["Observaciones"] = df["Observaciones"].fillna("-")
    df["Observaciones"] = df["Observaciones"].astype(str)
    if set(["Cultivo","Cód. Ceco","Ceco","Objetivo"]).issubset(df.columns):
         df = df.drop(columns=["Cultivo","Cód. Ceco","Ceco","Objetivo"],axis=1)
    df["Módulo Turno"] = df["Módulo Turno"].apply(format_lote)
    df["Módulo Turno"] = df["Módulo Turno"].replace({
        "LOTE ADMINISTRATIVO":"ADMINISTRATIVO",
        "LOTE 000":"LOTE 0"
    })
    df["Módulo"] = df["Módulo"].str.replace("MODULO ","M")
    df = df.rename(columns={"Módulo Turno":"LOTE","Módulo":"MODULO","Total Horas":"HORAS"})
    df.columns = [str(c).strip().upper() for c in df.columns]

    return df

#@st.cache_data(persist=True)
def agri_xlsx_data(path = "./data/download/"):
    
    data = pd.DataFrame()

    if os.path.exists(path):
        for file in os.listdir(path):
            if file.endswith(".xlsx"):
                print(file)
                df  = pd.read_excel(path + file,converters={"Total Horas":str})
                data = pd.concat([data, agritacer_data_detalle(df)], ignore_index=True)
    return data

def pipeline_agritracer():
    drive_id = "b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR"
    folder_id = "01XOBWFSG7XR5BMRB6XNAJBU7Q7KXU7BO3"
    download_files_c1()
    def historico_agritracer(access_token):
        data = listar_archivos_en_carpeta_compartida(
            access_token,
            drive_id,
            folder_id
        )
        url_parquet = get_download_url_by_name(data, "AGRITRACER_2025.parquet")
        #url_excel_2 = get_download_url_by_name(data, "REGISTRO APLICACIONES NUTRICIONALES-FUNDO QBERRIES.xlsx")
        return pd.read_parquet(url_parquet, engine="pyarrow")
    df = agri_xlsx_data()
    access_token = get_access_token()
    hdf = historico_agritracer(access_token)
    df = pd.concat([hdf, df], ignore_index=True)
    print(f"📤 Subiendo archivo 'AGRITRACER' a OneDrive...")
    
    resultado = subir_archivo_con_reintento(
        access_token=access_token,
        dataframe=df,
        nombre_archivo="AGRITRACER_GENERAL.parquet",
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

    
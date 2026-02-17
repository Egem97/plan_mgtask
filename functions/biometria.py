from utils.get_token import get_access_token
from utils.get_api import listar_archivos_en_carpeta_compartida,subir_archivo_con_reintento,get_tc_sunat_diario
from functions.proc_files_xlsx import agritacer_data_detalle,agri_xlsx_data
from utils.helpers import get_download_url_by_name
from utils.utils import *
from functions.recursos_humanos import read_costo_laboral

from functions.agricola import cosecha_datasets,clean_cosecha_2
import pandas as pd
import os
import streamlit as st
import numpy as np
from datetime import datetime



def clean_biometria_fundos():
    data = listar_archivos_en_carpeta_compartida(
        get_access_token(),
        "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        "01KM43WTYIAMKEILBABBD3YV6E7PSSUZLG"
    )
    data25_df = pd.read_excel(get_download_url_by_name(data, "BIOMETRIA CAM2025 FUN.xlsx"),sheet_name = "BD")
    data24_df = pd.read_excel(get_download_url_by_name(data, "BIOMETRIA CAM2024.xlsx"),sheet_name = "BD")
    #data26_canyon_df = pd.read_excel(get_download_url_by_name(data, "BIOMETRIA CANYON 2026.xlsx"),sheet_name = "CANYON")
    data25_df = pd.concat([data25_df,data24_df])#,data26_canyon_df
    st.write(f"JOIN WWW: {data25_df.shape}")
    st.dataframe(data25_df)
    data25_df.columns = (
        data25_df.columns.astype(str)
        .str.normalize('NFKD')
        .str.encode('ascii', errors='ignore')
        .str.decode('utf-8')
        .str.replace('\n', ' ', regex=False)
        .str.replace('.', '', regex=False)
        .str.strip()
    )
    data25_df = data25_df.loc[:, [c for c in data25_df.columns if not str(c).strip().upper().startswith("UNNAMED")]]

    columns25= [
        "FUNDO","ZONA","FECHA DE SIEMBRA","DDS","FECHA DE PODA","DDP","FECHA DE HOY","Difdias","SEMANA",
        "MODULO","TURNO","LOTE","VARIEDAD","ALTURA DE PLANTA","TASA DE CRECIMIENTO","N CANAS","N BROTES",
        "LONG BROTES","TC BROTE","N RETONOS","LONG BROTES RETONOS",
        "TC RETONOS","DIAMETRO","OBS"
    ]
    data25_df = data25_df[columns25]

    data25_df = data25_df.rename(columns={
        "Difdias": "DIFERENCIA DE DIAS",
        
        "N CANAS": "NUMERO DE CAÑAS",
        
        "N BROTES":"NUMERO DE BROTES",
        
        "LONG BROTES":"LONGITUD BROTES",
        "TC BROTE": "BROTES TASA DE CRECIMIENTO",
        "N RETONOS": "NUMERO DE RETOÑOS",
        "LONG BROTES RETONOS":"LONGITUD BROTES RETOÑO",
        "TC RETONOS":"RETOÑOS TASA DE CRECIMIENTO",
        "OBS":"OBSERVACION"


    })

    data25_df["FUNDO"] = data25_df["FUNDO"].fillna("NO ESPECIFICADO")
    data25_df["FUNDO"] = data25_df["FUNDO"].str.strip()
    data25_df["FUNDO"] = data25_df["FUNDO"].str.upper()
    data25_df["FUNDO"] = data25_df["FUNDO"].replace({"TARA":"LAS BRISAS","CANYON BERRIES":"EL POTRERO"})

    data25_df["ZONA"] = data25_df["ZONA"].fillna("NO ESPECIFICADO")
    data25_df["ZONA"] = data25_df["ZONA"].str.strip()
    data25_df["ZONA"] = data25_df["ZONA"].str.upper()

    data25_df["FECHA DE SIEMBRA"] = pd.to_datetime(data25_df["FECHA DE SIEMBRA"],errors="coerce").dt.date
    data25_df["FECHA DE PODA"] = pd.to_datetime(data25_df["FECHA DE PODA"],errors="coerce").dt.date
    data25_df["FECHA DE HOY"] = pd.to_datetime(data25_df["FECHA DE HOY"],errors="coerce").dt.date

    data25_df["MODULO"] = data25_df["MODULO"].fillna("X")
    data25_df["MODULO"] = data25_df["MODULO"].str.strip()
    data25_df["MODULO"] = data25_df["MODULO"].str.upper()
    data25_df["MODULO"] = data25_df["MODULO"].replace({
        "I": "M1",
        "II": "M2",
        "III": "M3"
    })
    data25_df["TURNO"] = data25_df["TURNO"].fillna(0)
    data25_df["LOTE"] = data25_df["LOTE"].fillna("x")
    data25_df["LOTE"] = data25_df["LOTE"].astype(str)
    data25_df["LOTE"] = data25_df["LOTE"].str.replace(".0","")
    data25_df["LOTE"] = data25_df["LOTE"].apply(format_lote)
    data25_df["OBSERVACION"] = data25_df["OBSERVACION"].fillna("-")
    var_numeric =['ALTURA DE PLANTA',  'TASA DE CRECIMIENTO', 'NUMERO DE CAÑAS',
         'NUMERO DE BROTES', 'LONGITUD BROTES', 'BROTES TASA DE CRECIMIENTO',
        'NUMERO DE RETOÑOS', 'LONGITUD BROTES RETOÑO','RETOÑOS TASA DE CRECIMIENTO', 'DIAMETRO'
    ]
    for col_num in var_numeric:
        data25_df[col_num] = pd.to_numeric(data25_df[col_num], errors='coerce').fillna(0)
    return data25_df

def clean_biometria_qberries():
    data = listar_archivos_en_carpeta_compartida(
            get_access_token(),
            "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
            "01KM43WTYIAMKEILBABBD3YV6E7PSSUZLG"
    )
    df = pd.read_excel(get_download_url_by_name(data, "Q BERRIES_BIOMETRIA 2025.xlsx"),sheet_name = "Registro",skiprows=1)
    df.columns = (
            df.columns.astype(str)
            .str.normalize('NFKD')
            .str.encode('ascii', errors='ignore')
            .str.decode('utf-8')
            .str.replace('\n', ' ', regex=False)
            .str.replace('.', '', regex=False)
            .str.strip()
            .str.upper()
        )
    df = df.loc[:, [c for c in df.columns if not str(c).strip().upper().startswith("UNNAMED")]]
    columns25_qberries = [

    "SEM",
    "VARIEDAD",
    "FECHA DE EVALUACION",
    "MODULO",
    "TURNO",
    "LOTE",
    "FECHA DE SIEMBRA",
    "DDS" ,
    "TALLOS/ PLANTA",
    "ALTURA DE PLANTA",
    "DIAS ENTRE EVAL",
    "TC PLANTA (CM/DIA)",
    "BROTES/ PLANTA",
    "RETONOS/ PLANTA",

    "ALTURA DE BROTE APICAL",
    "TC BROTE APICAL (CM/DIA)",
    "ALTURA DE BROTE BASAL",
    "TC BROTE BASAL (CM/DIA)2",
    "OBSERVACIONES"

    ]
    df = df[columns25_qberries]
    df = df.rename(columns = {
        "SEM" : "SEMANA",
        "DIAS ENTRE EVAL":"DIFERENCIA DE DIAS",
        "TALLOS/ PLANTA":"NUMERO DE CAÑAS",
        "TC PLANTA (CM/DIA)":"TASA DE CRECIMIENTO",
        "BROTES/ PLANTA":"NUMERO DE BROTES",
        "RETONOS/ PLANTA": "NUMERO DE RETOÑOS",

    })
    df["FECHA DE EVALUACION"] = pd.to_datetime(df["FECHA DE EVALUACION"],errors="coerce").dt.date
    df["FECHA DE SIEMBRA"] = pd.to_datetime(df["FECHA DE SIEMBRA"],errors="coerce").dt.date
    df["OBSERVACIONES"] = df["OBSERVACIONES"].fillna("-")
    df["OBSERVACIONES"] = df["OBSERVACIONES"].astype(str)
   

    df["MODULO"] = df["MODULO"].fillna("X")
    df["MODULO"] = df["MODULO"].astype(str)
    df["MODULO"] = "M"+df["MODULO"]
    return df


def qberries1_biometria_2026(data):

    df = pd.read_excel(get_download_url_by_name(
        data, 
        "REGISTRO DE BIOMETRÍA ETAPA O1_CAMPAÑA 2026.xlsx"),
        sheet_name = "REGISTRO",
        skiprows=1,
    )
    df["FUNDO"] = "LICAPA"
    df["FECHA DE PLANTACION"] = None
    df = df.rename(columns={"SDPO":"SPP",})
   
    return df

def qberries2_biometria_2026(data):

    df = pd.read_excel(get_download_url_by_name(
        data, 
        "REGISTRO DE BIOMETRÍA ETAPA O2_CAMPAÑA 2026.xlsx"),
        sheet_name = "REGISTRO",
        skiprows=1,
    )
    df["FUNDO"] = "LICAPA II"
    

    return df

#@st.cache_data(persist=True)
def biometria_2026(data):

    canyon_df = pd.read_excel(get_download_url_by_name(
        data, 
        "BIOMETRIA CANYON 2026.xlsx"),
        sheet_name = "CANYON",
    )
    canyon_df.columns = (
        canyon_df.columns.astype(str)
        .str.normalize('NFKD')
        .str.encode('ascii', errors='ignore')
        .str.decode('utf-8')
        .str.replace('\n', ' ', regex=False)
        .str.replace('.', '', regex=False)
        .str.strip()
    )
    gap_df = pd.read_excel(get_download_url_by_name(
        data, 
        "BIOMETRIA GAP BERRIES 2026.xlsx"),
        sheet_name = "REGISTRO",
    )
    gap_df.columns = (
        gap_df.columns.astype(str)
        .str.normalize('NFKD')
        .str.encode('ascii', errors='ignore')
        .str.decode('utf-8')
        .str.replace('\n', ' ', regex=False)
        .str.replace('.', '', regex=False)
        .str.strip()
    )
    #
    sj1_df = pd.read_excel(get_download_url_by_name(
        data, 
        "BIOMETRIA San Jose-2026.xlsx"),
        sheet_name = "REGISTRO",
    )
    sj1_df.columns = (
        sj1_df.columns.astype(str)
        .str.normalize('NFKD')
        .str.encode('ascii', errors='ignore')
        .str.decode('utf-8')
        .str.replace('\n', ' ', regex=False)
        .str.replace('.', '', regex=False)
        .str.strip()
    )
    sj2_df = pd.read_excel(get_download_url_by_name(
        data, 
        "BIOMETRIA San Jose II-2026.xlsx"),
        sheet_name = "REGISTRO",
    )
    sj2_df.columns = (
        sj2_df.columns.astype(str)
        .str.normalize('NFKD')
        .str.encode('ascii', errors='ignore')
        .str.decode('utf-8')
        .str.replace('\n', ' ', regex=False)
        .str.replace('.', '', regex=False)
        .str.strip()
    )

    sp_df = pd.read_excel(get_download_url_by_name(
        data, 
        "BIOMETRIA FUNDO SAN PEDRO 2026.xlsx"),
        sheet_name = "REGISTRO",
    )
    sp_df.columns = (
        sp_df.columns.astype(str)
        .str.normalize('NFKD')
        .str.encode('ascii', errors='ignore')
        .str.decode('utf-8')
        .str.replace('\n', ' ', regex=False)
        .str.replace('.', '', regex=False)
        .str.strip()
    )

    tara_df = pd.read_excel(get_download_url_by_name(
        data, 
        "BIOMETRIA TARA FARM 2026.xlsx"),
        sheet_name = "REGISTRO",
    )
    tara_df.columns = (
        tara_df.columns.astype(str)
        .str.normalize('NFKD')
        .str.encode('ascii', errors='ignore')
        .str.decode('utf-8')
        .str.replace('\n', ' ', regex=False)
        .str.replace('.', '', regex=False)
        .str.strip()
    )

    
    rename_source = {"LONG BROTES (F1)/CM":"LONG BROTES (F1)","TC BROTE (F1)/CM":"TC BROTE (F1)","OBS":"OBSERVACIONES"}
    sj1_df = sj1_df.rename(columns=rename_source)
    canyon_df = canyon_df.rename(columns=rename_source)
    gap_df = gap_df.rename(columns=rename_source)
    sp_df = sp_df.rename(columns=rename_source)
    sj2_df = sj2_df.rename(columns=rename_source)
    tara_df = tara_df.rename(columns=rename_source)

    sj1_df = sj1_df.drop(columns=["No DE BROTES TOTALES/CANAS"])
    dff = pd.concat([canyon_df,gap_df,sj1_df,sp_df,sj2_df,tara_df])
    dff = dff.drop(columns=["PRESENTACION (CC)","PRECENTACION (CC)"])
    dff = dff[
        (dff["FECHA DE EVALUACION"].notna())&
        (dff["MODULO"].notna())&
        (dff["TURNO"].notna())&
        (dff["LOTE"].notna())
    ]
    return dff


def pipeline_biometria():
    data = listar_archivos_en_carpeta_compartida(
        get_access_token(),
        "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        "01KM43WTYIAMKEILBABBD3YV6E7PSSUZLG"
    )
    qbe26_columns = [
        'FUNDO','ZONA',  'ANO', 'FECHA DE PLANTACION','SPP',    'EVALUACION ANTERIOR', 'FECHA DE EVALUACION',
        'Difdias', 'SEMANA', 'MODULO', 'TURNO', 'LOTE','VARIEDAD',
        
        'N CANAS',
        'TC BROTE (F1)', 'LONG BROTES (F1)', 'N BROTES (F1)','TC DE ALTURA PLANTA/CM','ALTURA DE PLANTA CM',
        'N BROTES (F2)', 'LONG BROTES (F2)',  'TC BROTE (F2)', 'N RETONOS', 
        'LONG BROTES RETONOS/CM', 
        'TC RETONOS/CM','DIAMETRO (MM)', 'BROTES TOTALES', 'OBSERVACIONES'
    ]
    #########################################################################################################    
    qberries_biometria1_26_df = qberries1_biometria_2026(data = data)
    
    #print("QBERRIES1")
    #print(list(qberries_biometria1_26_df.columns))
    qberries_biometria1_26_df.columns = (
            qberries_biometria1_26_df.columns.astype(str)
            .str.normalize('NFKD')
            .str.encode('ascii', errors='ignore')
            .str.decode('utf-8')
            .str.replace('\n', ' ', regex=False)
            .str.replace('.', '', regex=False)
            .str.strip()
        )
    qberries_biometria1_26_df = qberries_biometria1_26_df[
        (qberries_biometria1_26_df["FECHA DE EVALUACION"].notna())&
        (qberries_biometria1_26_df["MODULO"].notna())&
        (qberries_biometria1_26_df["TURNO"].notna())&
        (qberries_biometria1_26_df["LOTE"].notna())
    ]
    qberries_biometria1_26_df = qberries_biometria1_26_df.rename(columns={"SEM":"SEMANA","LONG BROTES (F1)/CM":"LONG BROTES (F1)","TC BROTE (F1)/CM":"TC BROTE (F1)"})
    qberries_biometria1_26_df["ZONA"] = "PAIJAN"
    if "SEMANA POST PODA" not in qberries_biometria1_26_df.columns:
        qberries_biometria1_26_df["SEMANA POST PODA"] = None
        qberries_biometria1_26_df = qberries_biometria1_26_df[qbe26_columns+['SEMANA POST PODA']]
    else:
        qberries_biometria1_26_df = qberries_biometria1_26_df[qbe26_columns+['SEMANA POST PODA']]
    ###################################################################################################
    
    qberries_biometria_26_df = qberries2_biometria_2026(data = data)    
    qberries_biometria_26_df.columns = (
            qberries_biometria_26_df.columns.astype(str)
            .str.normalize('NFKD')
            .str.encode('ascii', errors='ignore')
            .str.decode('utf-8')
            .str.replace('\n', ' ', regex=False)
            .str.replace('.', '', regex=False)
            .str.strip()
        )
    #print("QBERRIES2")
    #print(list(qberries_biometria_26_df.columns))
    qberries_biometria_26_df = qberries_biometria_26_df[
        (qberries_biometria_26_df["FECHA DE EVALUACION"].notna())&
        (qberries_biometria_26_df["MODULO"].notna())&
        (qberries_biometria_26_df["TURNO"].notna())&
        (qberries_biometria_26_df["LOTE"].notna())
    ]
    qberries_biometria_26_df = qberries_biometria_26_df.rename(columns={
        
        "DIF DIAS":"Difdias",
        "ALTURA DE PLANTA (cm)":"ALTURA DE PLANTA CM",
        "LONG BROTES RETONOS (cm)":"LONG BROTES RETONOS/CM",
        "TC RETONOS (cm)":"TC RETONOS/CM"
    })
    qberries_biometria_26_df = qberries_biometria_26_df.rename(columns={"SEM":"SEMANA","LONG BROTES (F1)/CM":"LONG BROTES (F1)","TC BROTE (F1)/CM":"TC BROTE (F1)"})
    qberries_biometria_26_df["ZONA"] = "PAIJAN"
    #['FECHA DE PODA', 'DDPO', 'RETONOS POR PLANTA']
    qbe26_columns = [
        'FUNDO','ZONA',  'ANO', 'FECHA DE PLANTACION','SPP',    'EVALUACION ANTERIOR', 'FECHA DE EVALUACION',
        'Difdias', 'SEMANA', 'MODULO', 'TURNO', 'LOTE','VARIEDAD',
        
        'N CANAS',
        'TC BROTE (F1)', 'LONG BROTES (F1)', 'N BROTES (F1)','TC DE ALTURA PLANTA/CM','ALTURA DE PLANTA CM',
        'N BROTES (F2)', 'LONG BROTES (F2)',  'TC BROTE (F2)', 'N RETONOS', 
        'LONG BROTES RETONOS/CM', 
        'TC RETONOS/CM','DIAMETRO (MM)', 'BROTES TOTALES', 'OBSERVACIONES'
    ]
    qberries_biometria_26_df = qberries_biometria_26_df[qbe26_columns]
    qberries_biometria_26_df["FECHA DE PODA"] = None
    
    ##################################################################################################33
    general_df = biometria_2026(data = data)
    general_df["SPP"] = None
    #print("FUNDOS")
    #print(list(general_df.columns))
    g26_columns = [
        'FUNDO', 'ZONA', 'ANO', 'FECHA DE PLANTACION','SPP', 'FECHA DE PODA',  'EVALUACION ANTERIOR', 'FECHA DE EVALUACION', 
        'Difdias', 'SEMANA','SEMANA POST PODA', 'MODULO', 'TURNO', 'LOTE', 'VARIEDAD', 
        'N CANAS', 'BROTES DE CANAS',
        'TC BROTE (F1)','LONG BROTES (F1)','N BROTES (F1)', 'TC DE ALTURA PLANTA/CM','ALTURA DE PLANTA CM',
        'N BROTES (F2)','LONG BROTES (F2)', 'TC BROTE (F2)','N RETONOS', 
        'LONG BROTES RETONOS/CM', 
        'TC RETONOS/CM','DIAMETRO (MM)', 'BROTES TOTALES', 'OBSERVACIONES']
    general_df= general_df[g26_columns]
    dff = pd.concat([qberries_biometria1_26_df,qberries_biometria_26_df,general_df])#,qberries_biometria_26_df
    dff = dff.rename(columns = {"ANO":"AÑO","Difdias":"DIFERENCIA DE DIAS"})
    dff["FUNDO"] = dff["FUNDO"].str.upper()
    dff["ZONA"] = dff["ZONA"].fillna("NO ESPECIFICADO")
    dff["ZONA"] = dff["ZONA"].str.upper()
    dff["FECHA DE PLANTACION"] = pd.to_datetime(dff["FECHA DE PLANTACION"], errors='coerce').dt.date
    dff["FECHA DE PODA"] = pd.to_datetime(dff["FECHA DE PODA"], errors='coerce').dt.date
    dff["FECHA DE EVALUACION"] = pd.to_datetime(dff["FECHA DE EVALUACION"], errors='coerce').dt.date
    dff["MODULO"] = dff["MODULO"].fillna("X")
    dff["MODULO"] = dff["MODULO"].astype(str)
    dff["MODULO"] = dff["MODULO"].str.strip()
    dff["MODULO"] = dff["MODULO"].str.upper()
    dff["MODULO"] = dff["MODULO"].replace("I", "1")
    dff["MODULO"] = dff["MODULO"].str.replace(".0", "")
    dff["MODULO"] = "M"+dff["MODULO"].astype(str)
    dff["TURNO"] = dff["TURNO"].fillna(0)
    dff["TURNO"] = dff["TURNO"].astype(str)
    dff["TURNO"] = dff["TURNO"].str.replace(".0","")
    dff["TURNO"] = "T"+dff["TURNO"]
    dff["LOTE"] = dff["LOTE"].fillna("x")
    dff["LOTE"] = dff["LOTE"].astype(str)
    dff["LOTE"] = dff["LOTE"].str.replace(".0","")
    dff["LOTE"] = dff["LOTE"].apply(format_lote)
    dff["VARIEDAD"] = dff["VARIEDAD"].fillna("NO ESPECIFICADO")
    dff["VARIEDAD"] = dff["VARIEDAD"].str.upper()
    cols_numeric = ['N CANAS',  'TC BROTE (F1)', 'LONG BROTES (F1)',
        'N BROTES (F1)', 'TC DE ALTURA PLANTA/CM', 'ALTURA DE PLANTA CM',
        'N BROTES (F2)', 'LONG BROTES (F2)', 'TC BROTE (F2)', 'N RETONOS',
        'LONG BROTES RETONOS/CM', 'TC RETONOS/CM', 'DIAMETRO (MM)',
        'BROTES TOTALES', 'OBSERVACIONES', 'BROTES DE CANAS',]
    for col_ in cols_numeric:
        dff[col_] = pd.to_numeric(dff[col_], errors='coerce').fillna(0)
    dff = dff.drop(columns = ["EVALUACION ANTERIOR"])
    
    return dff



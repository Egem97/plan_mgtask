import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import os
import re
from utils.utils import *

from utils.helpers import get_download_url_by_name
from utils.get_api import listar_archivos_en_carpeta_compartida
from utils.utils import read_excel_fast
from pandas.core.frame import DataFrame
from utils.get_token import get_access_token


def test1(access_token):
    data = listar_archivos_en_carpeta_compartida(
        access_token,
        "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        "01KM43WT22U7MUWEHXNRBZBLP6734DCWJO"
    )
    url_excel_1 = get_download_url_by_name(data, "REGISTRO GENERAL DE APLICACIONES NUTRICIONALES.xlsx")
    #url_excel_2 = get_download_url_by_name(data, "REGISTRO APLICACIONES NUTRICIONALES-FUNDO QBERRIES.xlsx")
    return read_excel_fast(url_excel_1, sheet_name="BD APLICACION NUTRICIONAL ", skiprows=1)  #, pd.read_excel(url_excel_2, sheet_name="BD")


def test2(access_token):
    data = listar_archivos_en_carpeta_compartida(
        access_token,
        "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        "01KM43WT22U7MUWEHXNRBZBLP6734DCWJO"
    )
    #url_excel_1 = get_download_url_by_name(data, "REGISTRO GENERAL DE APLICACIONES NUTRICIONALES.xlsx")
    url_excel_2 = get_download_url_by_name(data, "REGISTRO APLICACIONES NUTRICIONALES-FUNDO QBERRIES.xlsx")
    return read_excel_fast(url_excel_2, sheet_name="BD", skiprows=1)


def fertiriegoQberries(access_token):
    data = listar_archivos_en_carpeta_compartida(
        access_token,
        "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        "01KM43WT22U7MUWEHXNRBZBLP6734DCWJO"
    )
    url_excel_1 = get_download_url_by_name(data, "REGISTRO DE RIEGO Y FERTIRRIEGO-FUNDO QBERRIES.xlsx")
    #url_excel_2 = get_download_url_by_name(data, "REGISTRO APLICACIONES NUTRICIONALES-FUNDO QBERRIES.xlsx")
    return read_excel_fast(url_excel_1, sheet_name="BD FERTIRRIEGO ", skiprows=1)

#@st.cache_data(ttl=60*60)
def fertiriegoGeneral(access_token):
    #data = listar_archivos_en_carpeta_compartida(
    #    access_token,
    #    "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
    #    "01KM43WT22U7MUWEHXNRBZBLP6734DCWJO"
    #)

    #return read_excel_fast(url_excel_1, sheet_name="BASE DE DATOS FERTIRRIEGO ", skiprows=1)
    path = r"C:\Users\EdwardoGiampiereEnri\OneDrive - ALZA PERU GROUP S.A.C\Archivos de Andy Rodriguez - INTELIGENCIA DE NEGOCIOS\DATA BI\AGRICOLA\RIEGO Y FERTIRIEGO"
    return pd.read_excel(path + "\REGISTRO GENERAL DE RIEGO Y FERTIRRIEGO.xlsx", sheet_name="BASE DE DATOS FERTIRRIEGO ", skiprows=1)


def ph_ce_general(access_token):
    data = listar_archivos_en_carpeta_compartida(
        access_token,
        "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        "01KM43WT22U7MUWEHXNRBZBLP6734DCWJO"
    )
    url_excel_1 = get_download_url_by_name(data, "REGISTRO GENERAL DE PH Y CE.xlsx")
    
    return read_excel_fast(url_excel_1, sheet_name="Hoja1", skiprows=2)

def ph_ce_qberries(access_token):
    data = listar_archivos_en_carpeta_compartida(
        access_token,
        "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        "01KM43WT22U7MUWEHXNRBZBLP6734DCWJO"
    )
    url_excel_1 = get_download_url_by_name(data, "REGISTRO PH Y CE - FUNDO Q BERRIES.xlsx")
    
    return read_excel_fast(url_excel_1, sheet_name="PH Y CE", skiprows=1)


def drenaje_data(access_token):
    data = listar_archivos_en_carpeta_compartida(
        access_token,
        "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        "01KM43WT22U7MUWEHXNRBZBLP6734DCWJO"
    )
    url_excel_1 = get_download_url_by_name(data, "REGISTRO GENERAL DE DRENAJES.xlsx")
    
    return read_excel_fast(url_excel_1, sheet_name="DATA DRENAJE", skiprows=1)


def cosecha_datasets(access_token,name_excel,preferred_sheet,skip_rows):
    data = listar_archivos_en_carpeta_compartida(
        access_token,
        "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        "01KM43WT3E3DVAFNF4ZRGJS66GDVM3ZRMO"
    )
    url_excel_1 = get_download_url_by_name(data, name_excel)
    
    return read_excel_fast(url_excel_1,sheet_name=preferred_sheet, skiprows=skip_rows)#, sheet_name="DATA DRENAJE", skiprows=1


def aplicativoNutricional():
    access_token = get_access_token()
    nutri_general_df = test1(access_token)
    nutri_qb_df = test2(access_token)
    df = pd.concat([nutri_general_df,nutri_qb_df],axis=0)
    df = df.loc[:, [c for c in df.columns if not str(c).strip().upper().startswith("UNNAMED")]]
    df.columns = [str(c).strip().upper() for c in df.columns]
    df["INGREDIENTE ACTIVO"] = df["INGREDIENTE ACTIVO"].fillna("NO ESPECIFICADO")
    df["RESPONSABLE"] = df["RESPONSABLE"].fillna("NO ESPECIFICADO")
    df["OBSERVACION"] = df["OBSERVACION"].fillna("-")
    df["VOLUMEN"] = df["VOLUMEN"].fillna(0)
    df["VOLUMEN"] = df["VOLUMEN"].replace(" ",0)
    df["CANTIDAD TOTAL"] = df["CANTIDAD TOTAL"].fillna(0)
    df["CANTIDAD TOTAL"] = df["CANTIDAD TOTAL"].replace(" ",0)
    df["FUNDO"] = df["FUNDO"].fillna("NO ESPECIFICADO")
    df["FUNDO"] = df["FUNDO"].str.upper()
    df["FUNDO"] = df["FUNDO"].str.strip()
    df["FUNDO"] = df["FUNDO"].replace("QBERRIES","LICAPA")

    df["LOTE"] = df["LOTE"].astype(str)
    df["LOTE"] = df["LOTE"].apply(format_lote)

    df["UNIDAD"] = df["UNIDAD"].fillna("-")
    df["UNIDAD"] = df["UNIDAD"].str.strip()
    df["UNIDAD"] = df["UNIDAD"].str.upper()

    df["FECHA"] = pd.to_datetime(df["FECHA"]).dt.date
    df["VIA DE APLICACIÓN"] = df["VIA DE APLICACIÓN"].str.strip()
    df["VIA DE APLICACIÓN"] = df["VIA DE APLICACIÓN"].replace(
        {
            "FUMIGACION":"FOLIAR",
            "FUMIGACIÓN":"FOLIAR",
            "FUMIGACION-ADICIONAL":"FOLIAR"
        }
    )
    df["MES"] = df["MES"].str.upper()
    
    return df


def FertiRiego():
    access_token = get_access_token()
    fertiriego_qberries_df = fertiriegoQberries(access_token)
    fertiriego_general_df = fertiriegoGeneral(access_token)
    fertiriego_qberries_df = fertiriego_qberries_df.loc[:, [c for c in fertiriego_qberries_df.columns if not str(c).strip().upper().startswith("UNNAMED")]]
    fertiriego_general_df = fertiriego_general_df.loc[:, [c for c in fertiriego_general_df.columns if not str(c).strip().upper().startswith("UNNAMED")]]

    fertiriego_qberries_df.columns = [str(c).strip().upper() for c in fertiriego_qberries_df.columns]
    fertiriego_general_df.columns = [str(c).strip().upper() for c in fertiriego_general_df.columns]
    fertiriego_df = pd.concat([fertiriego_qberries_df,fertiriego_general_df],axis=0)
    fertiriego_df = fertiriego_df[fertiriego_df["FECHA"].notna()]
    fertiriego_df["TURNO ORIGINAL"] = fertiriego_df["TURNO"]
    fertiriego_df["TURNO ORIGINAL"] = fertiriego_df["TURNO ORIGINAL"].fillna("0")
    fertiriego_df["TURNO ORIGINAL"] = fertiriego_df["TURNO ORIGINAL"].astype(str)
    fertiriego_df["TURNO"] = fertiriego_df["TURNO"].fillna(99)
    fertiriego_df = fertiriego_df.drop(columns=["TOTAL (MIN)"])

    fertiriego_df["MES"] = fertiriego_df["MES"].str.upper()
    fertiriego_df["MODULO"] = fertiriego_df["MODULO"].fillna("X")
    fertiriego_df["MODULO"] = fertiriego_df["MODULO"].astype(str)
    fertiriego_df["MODULO"] = 'M' +fertiriego_df["MODULO"].astype(str)
    fertiriego_df["MODULO"] = fertiriego_df["MODULO"].str.replace(".0","")
    fertiriego_df["TURNO"] = fertiriego_df["TURNO"].apply(clean_turno).astype(int)
    fertiriego_df["TURNO"] = fertiriego_df["TURNO"].astype(int)

    fertiriego_df["FASE"] = fertiriego_df["FASE"].fillna("NO ESPECIFICADO")
    fertiriego_df["FASE"] = fertiriego_df["FASE"].str.strip()
    fertiriego_df["EQUIPO"] = fertiriego_df["EQUIPO"].fillna("NO ESPECIFICADO")
    fertiriego_df["EQUIPO"] = fertiriego_df["EQUIPO"].str.strip()
    fertiriego_df["SUPERVISOR"] = fertiriego_df["SUPERVISOR"].fillna("NO ESPECIFICADO")
    fertiriego_df["SUPERVISOR"] = fertiriego_df["SUPERVISOR"].str.strip()
    fertiriego_df["DESCRIPCIÓN"] = fertiriego_df["DESCRIPCIÓN"].fillna("NO ESPECIFICADO")   
    fertiriego_df["DESCRIPCIÓN"] = fertiriego_df["DESCRIPCIÓN"].str.strip()

    fertiriego_df["AREA"] = fertiriego_df["AREA"].fillna("0")
    fertiriego_df["AREA"] = fertiriego_df["AREA"].astype(str)
    fertiriego_df["AREA"] = fertiriego_df["AREA"].str.strip()
    fertiriego_df["AREA"] = fertiriego_df["AREA"].replace("","0")
    fertiriego_df["AREA"] = fertiriego_df["AREA"].replace("-","0")
    fertiriego_df["AREA"] = fertiriego_df["AREA"].astype(float)

    fertiriego_df["VARIEDAD"] = fertiriego_df["VARIEDAD"].fillna("NO ESPECIFICADO")
    fertiriego_df["VARIEDAD"] = fertiriego_df["VARIEDAD"].str.strip()
    fertiriego_df["VARIEDAD"] = fertiriego_df["VARIEDAD"].str.upper()
    fertiriego_df["VARIEDAD"] = fertiriego_df["VARIEDAD"].replace("SEKOYA","SEKOYA POP")
    fertiriego_df["FUNDO"] = fertiriego_df["FUNDO"].fillna("NO ESPECIFICADO")
    fertiriego_df["FUNDO"] = fertiriego_df["FUNDO"].str.strip()
    fertiriego_df["FUNDO"] = fertiriego_df["FUNDO"].str.upper()
    fertiriego_df["FUNDO"] = fertiriego_df["FUNDO"].replace({"QBERRIES":"LICAPA","CANYON BERRIES":"EL POTRERO","QBERRIES II":"LICAPA II"})
    fertiriego_df["FECHA"] = pd.to_datetime(fertiriego_df["FECHA"]).dt.date

    var_float = ['LECTURA DE HIDROMETRO INICIAL',
        'LECTURA DE HIDROMETRO FINAL', 'AGUA PROGRAMADA (M3)', 'ETO',
        'LAMINA (MM)', '% REPOSICION', 'TANQ. 1', 'TANQ. 2', 'TANQ. 3',
        'TANQ. 4', 'TANQ. 5', 'TANQ. 6', 'TANQ.7',
        'SULFATO DE AMONIO (N) 21 %N - 0 - 24 %S',
        'NITRATO DE AMONIO (N) 0 - 33 %N - 3 %P2O5',
        'ACIDO FOSFORICO (P) 0 - 60 %P - 0',
        'SULFATO DE POTASIO (K) 0 - 0 - 50 %K2O',
        'SULFATO DE MAGNESIO (MG) 16 %MGO',
        'NITRATO DE CALCIO (CA) 15 %N - 0 -0   24 %CA',
        'FOSFATO MONOPOTASICO (0-52-34)',
        'FOSFATO MONOAMONICO 12%N - 61%P2O5 - 0',
        'NITRATO DE POTASIO (13.5-0-46)', 'SULFATO DE ZINC', 'QUELATOS', 'N',
        'P', 'K', 'MG', 'CA', 'ZN', 'FE', 'S', 'SULFATO DE MANGANESO',
        'NANOFERT NITRO N 8%', 'EPSOTOP MGO 16%  S 13%',
        'ALLGANIC POTASSIUM K2O 52% S 18%', 'ORGANICHEM CALCIO 23% CAO', 'MN',
    ]

    for c in var_float:
        fertiriego_df[c] = fertiriego_df[c].fillna("0")
        fertiriego_df[c] = fertiriego_df[c].astype(str)
        fertiriego_df[c] = fertiriego_df[c].str.strip()
        fertiriego_df[c] = fertiriego_df[c].replace("","0")
        fertiriego_df[c] = fertiriego_df[c].replace("-","0")
        fertiriego_df[c] = fertiriego_df[c].astype(float)
    fertiriego_df = fertiriego_df.drop(columns=["TURNO ORIGINAL","AÑO","SEMANA"])
    fertiriego_df["TURNO"] = fertiriego_df["TURNO"].astype(str)
    mask_gap = fertiriego_df["FUNDO"] == "GAP BERRIES"
    fertiriego_df.loc[mask_gap & fertiriego_df["TURNO"].isin(["1","2","3","4"]), "TURNO"] = "1"
    fertiriego_df.loc[mask_gap & fertiriego_df["TURNO"].isin(["5","6","7","8"]), "TURNO"] = "2"
    fertiriego_df.loc[mask_gap & fertiriego_df["TURNO"].isin(["9","10","11"]), "TURNO"] = "3"
    fertiriego_df.loc[mask_gap & fertiriego_df["TURNO"].isin(["12","13","14"]), "TURNO"] = "4"
    fertiriego_df.loc[mask_gap & fertiriego_df["TURNO"].isin(["15","16","17"]), "TURNO"] = "5"
    fertiriego_df.loc[mask_gap & fertiriego_df["TURNO"].isin(["18","19","20"]), "TURNO"] = "6"
    fertiriego_df.loc[mask_gap & fertiriego_df["TURNO"].isin(["21","22","23"]), "TURNO"] = "7"
    fertiriego_df.loc[mask_gap & fertiriego_df["TURNO"].isin(["24","25","26"]), "TURNO"] = "8"
    fertiriego_df.loc[mask_gap & fertiriego_df["TURNO"].isin(["27","28"]), "TURNO"] = "9"
    fertiriego_df.loc[mask_gap & fertiriego_df["TURNO"].isin(["29","30","31"]), "TURNO"] = "10"

    mask_colina = fertiriego_df["FUNDO"] == "LA COLINA"
    fertiriego_df.loc[mask_colina & fertiriego_df["TURNO"].isin(["1","2","3","4","5","6","7","8","9","10"]), "TURNO"] = "1"
    #fertiriego_df["FECHA"] = pd.to_datetime(fertiriego_df["FECHA"])
    return fertiriego_df



#89
#@st.cache_data(ttl=60*5)
def parametros_campo():
    access_token = get_access_token()
    df = ph_ce_general(access_token)
    dff = ph_ce_qberries(access_token)
    df_general = pd.concat([df,dff],axis=0)
    df_general = df_general.loc[:, [c for c in df_general.columns if not str(c).strip().upper().startswith("UNNAMED")]]
    df_general.columns = [str(c).strip().upper() for c in df_general.columns]
    df_general["FUNDO"] = df_general["FUNDO"].fillna("NO ESPECIFICADO")
    df_general["FUNDO"] = df_general["FUNDO"].str.strip()
    df_general["TIPO DE MUESTRA"] = df_general["TIPO DE MUESTRA"].fillna("NO ESPECIFICADO")
    df_general["TIPO DE MUESTRA"] = df_general["TIPO DE MUESTRA"].str.strip()
    df_general["VARIEDAD"] = df_general["VARIEDAD"].fillna("NO ESPECIFICADO")
    df_general["VARIEDAD"] = df_general["VARIEDAD"].str.strip()
    df_general["FUNDO"] = df_general["FUNDO"].fillna("NO ESPECIFICADO")
    df_general["FUNDO"] = df_general["FUNDO"].str.strip()
    df_general["FUNDO"] = df_general["FUNDO"].str.upper()
    df_general["FUNDO"] = df_general["FUNDO"].replace({"CANYON BERRIES":"EL POTRERO","QBERRIES":"LICAPA"})
    df_general["MES"] = df_general["MES"].fillna("NO ESPECIFICADO")
    df_general["MES"] = df_general["MES"].str.strip()
    df_general["MES"] = df_general["MES"].str.upper()
    df_general["MES"] = df_general["MES"].str.upper()

    df_general["PROFUNDIDAD"] = df_general["PROFUNDIDAD"].fillna("0")
    # Extraer sólo números de PROFUNDIDAD (ej. '15cm' -> 15)
    df_general["PROFUNDIDAD"] = (
        df_general["PROFUNDIDAD"].astype(str)
        .str.extract(r"(\d+(?:\.\d+)?)", expand=False)
        .fillna("0")
        .astype(float)
        .astype(int)
    )
    df_general["MODULO"] = df_general["MODULO"].fillna("MX")
    df_general["TURNO"] = df_general["TURNO"].fillna("99")
    df_general["TURNO"] = df_general["TURNO"].astype(str)
    mask_colina_ = df_general["FUNDO"] == "LA COLINA"
    df_general.loc[mask_colina_ & df_general["TURNO"].isin(["1","2","3","4","5","6","7","8","9","10"]), "TURNO"] = "1"
    
    df_general["PARAMETRO"] = df_general["PARAMETRO"].fillna("XX")
    df_general["LECTURA"] = df_general["LECTURA"].fillna(0)
    df_general["L MIN"] = df_general["L MIN"].fillna(0)
    df_general["L MAX"] = df_general["L MAX"].fillna(0)
    df_general["FECHA"] = pd.to_datetime(df_general["FECHA"]).dt.date
    df_general["TIPO DE MUESTRA"] = df_general["TIPO DE MUESTRA"].str.upper()
    df_general["INDEX"] = df_general["TIPO DE MUESTRA"].replace({
        "CANAL":1,
        "DECANTADOR":2,
        "REACTOR":3,
        "RESERVORIO":4,
        "FILTRADO":5,
        "SFR":6,
        "SS":7,
        "DRENAJE":8,
        "SUSTRATO":9,
        "SOL. FIBRA COCO":10,
        "POZO 01":11,
        "OSMOSIS":12,
        "GOTERO":13,
        
        "SALMUERA":14,
        "POZO 02":15,
        "POZO 3":16,
        "POZO 4":17,
        "POZO EXISTENTE":18,
        "OSMOSIS 1":19,
        "OSMOSIS 2":20,
    })
    
    return df_general

#df_muestras = parametros_campo()
#st.write(df_muestras.shape)
#st.dataframe(df_muestras)
#print(df_muestras.info())
#print(df_muestras["FUNDO"].unique())
def drenaje_campo():
    access_token = get_access_token()
    df = drenaje_data(access_token)
    df = df.loc[:, [c for c in df.columns if not str(c).strip().upper().startswith("UNNAMED")]]
    df.columns = [str(c).strip().upper() for c in df.columns]
    df = df[df["FECHA"].notna()]
    df = df.drop(columns=["COLUMNA1","COLUMNA2"])
    df["FUNDO"] = df["FUNDO"].fillna("NO ESPECIFICADO")
    df["FUNDO"] = df["FUNDO"].str.strip()
    df["FUNDO"] = df["FUNDO"].str.upper()
    df["FUNDO"] = df["FUNDO"].replace({"CANYON BERRIES":"EL POTRERO","QBERRIES":"LICAPA","QBERRIES II":"LICAPA II"})

    df["MODULO"] = df["MODULO"].fillna("MX")
    df["MODULO"] = df["MODULO"].str.strip()
    df["MODULO"] = df["MODULO"].str.upper()

    # Regla: si FUNDO == "SAN JOSE" y MODULO == "M2", cambiar FUNDO a "SAN JOSE II"
    df.loc[(df["FUNDO"] == "SAN JOSE") & (df["MODULO"] == "M2"), "FUNDO"] = "SAN JOSE II"


    df["TURNO"] = df["TURNO"].fillna(99)
    df["TURNO"] = df["TURNO"].astype(str)
    df["TURNO"] = df["TURNO"].str.strip()

    df["TURNO"] = df["TURNO"].replace("","0")
    df["TURNO"] = df["TURNO"].replace("-","0")

    df["VARIEDAD"] = df["VARIEDAD"].fillna("NO ESPECIFICADO")
    df["VARIEDAD"] = df["VARIEDAD"].str.strip()
    df["VARIEDAD"] = df["VARIEDAD"].str.upper()

    df["VALVULA"] = df["VALVULA"].fillna(0)

    df["UBICACIÓN"] = df["UBICACIÓN"].fillna("NO ESPECIFICADO")
    df["UBICACIÓN"] = df["UBICACIÓN"].str.strip()
    df["UBICACIÓN"] = df["UBICACIÓN"].str.upper()
    cols_num_drenaje = ['ETO MM/DIA', 'LÁMINA(MM)', 'REPOSICIÓN MM',
        'VOL DREN.1', 'VOL DREN. 2', 'VOLUMEN AFORO', '% DRENAJE REAL',
        '% MÍNIMO', '% MÁXIMO']
    #import streamlit as st
    #st.write(df)
    for c in cols_num_drenaje:
        print(c)
        df[c] = df[c].fillna(0)
        df[c] = df[c].astype(float)

    df["FECHA"] = pd.to_datetime(df["FECHA"]).dt.date
    return df


def data_cosecha():
    #path_cosecha = r"C:\Users\EdwardoGiampiereEnri\OneDrive - ALZA PERU GROUP S.A.C\Archivos de Andy Rodriguez - INTELIGENCIA DE NEGOCIOS\DATA BI\AGRICOLA\PRODUCCION"
    access_token = get_access_token()
    list_files = [
        "1. Cosecha Excelence Sur 2025 CAMPO San Jose I.xlsx",
        "2. Cosecha Excelence Sur 2025 CAMPO San Jose II LIS (1).xlsx",
        "5. Cosecha QBERRIES-CAMPAÑA-2025.xlsx",
        "3. Cosecha GAP - 2025.xlsx",
        "4. Cosecha TARA FARM - 2025.xlsx",
        "COSECHA FUNDO SAN PEDRO 2025 ACTUALIZADO.xlsx",
        
        "COSECHA CANYON BERRIES 2025 ACTUALIZADO.xlsx",
        "REPORTE COSECHA LA COLINA ATLAS 2025..xlsx"

        ]

    data = pd.DataFrame()
    for file in list_files:
  
        # Hoja preferida por archivo
        #DATA EXP Y CAMP 
        if file == "REPORTE COSECHA LA COLINA ATLAS 2025..xlsx":
            preferred_sheet = "KG VARIEDAD"
        elif file == "3. Cosecha GAP - 2025.xlsx":
            preferred_sheet = "DATA EXP Y CAMP "
        elif file == "5. Cosecha QBERRIES-CAMPAÑA-2025.xlsx":
            preferred_sheet = "DATA EXP Y CAMP  "
        else:
            preferred_sheet = "DATA EXP Y CAMP"

        if file == "COSECHA CANYON BERRIES 2025 ACTUALIZADO.xlsx":
            skip_rows = 12
        elif file == "REPORTE COSECHA LA COLINA ATLAS 2025..xlsx":
            skip_rows = 4
        elif file == "COSECHA FUNDO SAN PEDRO 2025 ACTUALIZADO.xlsx":
            skip_rows = 12

        else:
            skip_rows = None
        # Lectura robusta con saneo del Excel si el XML está inválido
        #dff = pd.read_excel(full_path, sheet_name=preferred_sheet, skiprows=skip_rows)
        #st.write(dff.shape)
        df = cosecha_datasets(access_token,file,preferred_sheet,skip_rows)
        #st.write(full_path)
        #st.dataframe(df)
        if file == "COSECHA CANYON BERRIES 2025 ACTUALIZADO.xlsx" or file == "COSECHA FUNDO SAN PEDRO 2025 ACTUALIZADO.xlsx":
            df = df.rename(columns={"KILOS": "KILOS BRUTOS","º":"MES"})
        # Si no se pudo leer, avisar y continuar con el siguiente archivo
        #if df is None:
        #    st.warning(f"No se pudo leer '{file}' (hoja: {preferred_sheet}).")
        #    continue
        
        # Limpieza por iteración: quitar columnas 'Unnamed*' y estandarizar nombres
        df = df.loc[:, [c for c in df.columns if not str(c).strip().upper().startswith("UNNAMED")]]
        df.columns = [str(c).strip().upper() for c in df.columns]
        if file == "REPORTE COSECHA LA COLINA ATLAS 2025..xlsx":
            df = lacolina_transform(df)
        
        data = pd.concat([data, df], axis=0)

    # Convertir FECHA (puede venir como serial Excel tipo "45968") a datetime
    data["FECHA"] = data["FECHA"].apply(parse_mixed_date)
    data = data[data["FECHA"].notna()]
    data["VARIEDAD"] = data["VARIEDAD"].fillna("NO ESPECIFICADO")
    data["VARIEDAD"] = data["VARIEDAD"].replace(0,"NO ESPECIFICADO")
    data["VARIEDAD"] = data["VARIEDAD"].astype(str)
    data["Nª GUIA INTERNA"] = data["Nª GUIA INTERNA"].fillna("xxxx")
    data["Nª GUIA INTERNA"] = data["Nª GUIA INTERNA"].astype(str)

    data["LOTE"] = data["LOTE"].fillna("x")
    data["LOTE"] = data["LOTE"].astype(str)
    data["LOTE"] = data["LOTE"].str.replace(".0","")
    data["LOTE"] = data["LOTE"].apply(format_lote)

    data["DESCARTE"] = data["DESCARTE"].fillna(0)
    data["DESCARTE"] = data["DESCARTE"].replace(".",0)

    data["FUNDO"] = data["FUNDO"].fillna("NO ESPECIFICADO")
    data["FUNDO"] = data["FUNDO"].str.strip()
    data["FUNDO"] = data["FUNDO"].str.upper()
    data["FUNDO"] = data["FUNDO"].replace(
        {
            "GAP":"GAP BERRIES",
            "QBERRIES":"LICAPA",
            "CANYON BERRIES":"EL POTRERO",
        }
    )
    data = data.drop(columns = ["Nª GUIA INTERNA","MES","SEMANA","REF","REF2","MERCADO"])

    data["MODULO"] = data["MODULO"].fillna("MX")
    data["MODULO"] = data["MODULO"].replace({"I":"M1","II":"M2"})
    
    data["TURNO"] = data["TURNO"].fillna(0)
    cols_numeric = ['HA REAL', 'KG/HA REAL', 'HA', 'JORNAL', 'JABAS', 'JARRAS',
        'KILOS BRUTOS', 'KILOS /HA', 'JARRAS/JR', 'KG/JR', 'DESCARTE']
    for col in cols_numeric:
        data[col] = data[col].fillna(0)
    data["FECHA"] = pd.to_datetime(data["FECHA"]).dt.date
    return data

#DATA QUE CAMBIA POCAS VECES O FRECUENCIA

def sede():
    sedes = ['SAN JOSE', 'SAN PEDRO', 'SAN JOSE II','LAS BRISAS','LICAPA','EL POTRERO','GAP BERRIES','LA COLINA']
    zonas = ['SUR','NORTE','SUR','SUR','PAIJAN','NORTE','SUR','LA COLINA']
    sedes_df = pd.DataFrame(sedes, columns=['SEDE'])
    sedes_df["Zona"] = zonas
    return sedes_df


def poligonos(access_token):
    data = listar_archivos_en_carpeta_compartida(
        access_token,
        #"b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        "b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR",
        "01XOBWFSG3TINCMZG2RVHIU5OP6DGFNVX3"
    )
    url_excel_1 = get_download_url_by_name(data, "poligonos.xlsx")
    xls = pd.ExcelFile(url_excel_1)
    list_hojas = list(xls.sheet_names)
    data = pd.DataFrame()
    for hoja in list_hojas:
        df = pd.read_excel(url_excel_1, sheet_name=hoja)
        df["Fundo"] = hoja

        #wkt_col = detect_wkt_column(df)
        #if wkt_col:
        #    df["GeoJSON"] = df[wkt_col].apply(lambda s: json.dumps(wkt_to_geojson_dict(s)) if wkt_to_geojson_dict(s) else None)
        #else:
        #    st.warning(f"No se encontró columna WKT en la hoja '{hoja}'.")
        
        data = pd.concat([data,df],axis=0)
    return data

def informe_plantacion(access_token):
    data = listar_archivos_en_carpeta_compartida(
        access_token,
        "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        "01KM43WT3E3DVAFNF4ZRGJS66GDVM3ZRMO"
    )
    url_excel_1 = get_download_url_by_name(data, "Informe de Plantación - 2025 Zona Sur y Norte V1.xlsx")
    url_excel_2 = get_download_url_by_name(data, "Informe de Plantación - 2025 Qberries V1.xlsx")

    columns_data = ['Zona', 'Fundo', 'Predio', 'Módulo', 'Turno', 'Lote',
       'Área actualizada', 'Área sin plantas', 'Año', 'Sem',
       'Fecha de plantación', 'Variedad', 'Presentación (CC)', 'Estatus',
       'Origen', 'Jefe Zona ', 'Jefe de Fundo', 'Dist. surcos (m)',
       'Dist. entre plantas (m)', 'M2', 'Plantas/ha', 'N° de Plantas ',
       'Diametro de lineas (mm)', 'N° Lineas', 'Distancia goteros',
       'Caudal nominal lts/hr', 'Goteros planta', 'Precipitación mm/Horas/ha',
       'Tipo de manguera']  
    # Leer Informe de Plantación Zona Sur y Norte
    sn_dff = read_excel_resilient(
        url_excel_1, sheet_name="BDIP", skiprows=5, engine="openpyxl"
    )
    sn_dff = sn_dff[columns_data]
    sn_dff = sn_dff[sn_dff["Lote"].notna()]

    lc_lotes = [1,2,3,4,5,6,7,8,9,10]
    lc_ha = [1.24,0.40,1.23,0.99,1.11,1.26,1.10,0.81,0.65,0.40]

    # Crear dataframe con las dos listas
    lc_dff = pd.DataFrame({'Lote': lc_lotes, 'Área actualizada': lc_ha})

    lc_dff["Zona"] = "LA COLINA"
    lc_dff["Fundo"] = "LA COLINA"
    lc_dff["Predio"] = "LA COLINA MOD-"+lc_dff["Lote"].astype(str)
    lc_dff["Módulo"] = "M1"
    lc_dff["Turno"] = 1
    lc_dff["Área sin plantas"] = 0
    lc_dff["Año"] = 2025
    lc_dff["Sem"] = 0
    lc_dff["Fecha de plantación"] = datetime(2025, 1, 1)
    lc_dff["Variedad"] = "ATLAS"
    lc_dff["Presentación (CC)"] = 1000
    lc_dff["Estatus"] = "BOLSA"
    lc_dff["Origen"] = "NO ESPECIFICADO"
    lc_dff["Jefe Zona "] = "NO ESPECIFICADO"
    lc_dff["Jefe de Fundo"] = "NO ESPECIFICADO"
    lc_dff["Dist. surcos (m)"] = 0
    lc_dff["Dist. entre plantas (m)"] = 0
    lc_dff["M2"] = 0
    lc_dff["Plantas/ha"] = 6500
    lc_dff["N° de Plantas "] = lc_dff["Plantas/ha"] * lc_dff["Área actualizada"]
    lc_dff["Diametro de lineas (mm)"] = 0
    lc_dff["N° Lineas"] = 1
    lc_dff["Distancia goteros"] = 0.15
    lc_dff["Caudal nominal lts/hr"] = 1.6
    lc_dff["Goteros planta"] = 3
    lc_dff["Precipitación mm/Horas/ha"] = 0
    lc_dff["Tipo de manguera"] = "BICAPA"

    qberries_dff = read_excel_resilient(
        url_excel_2, sheet_name="BDIP", skiprows=5, engine="openpyxl"
    )
    qberries_dff = qberries_dff[columns_data]
    qberries_dff = qberries_dff[qberries_dff["Lote"].notna()]
    qberries_dff["Lote"] = qberries_dff["Lote"].astype(int).astype(str)

    dff = pd.concat([sn_dff, qberries_dff,lc_dff], axis=0)
    dff = dff[dff["Lote"].notna()]
    dff["Lote"] = dff["Lote"].astype(str)
    dff["Área sin plantas"] = dff["Área sin plantas"].fillna(0)

    dff.columns = [strip_accents(col).strip().upper() for col in dff.columns]
    for col in ['ZONA', 'FUNDO', 'PREDIO', 'MODULO','VARIEDAD','ESTATUS','ORIGEN','JEFE ZONA','JEFE DE FUNDO','TIPO DE MANGUERA']:
            
        if dff[col].dtype == "object":
            dff[col] = dff[col].fillna("NO ESPECIFICADO")
            dff[col] = dff[col].str.strip()
            dff[col] = dff[col].str.upper()

    # Aplicar normalización de fecha y convertir a tipo fecha (YYYY-MM-DD)
    dff["MODULO"] = dff["MODULO"].str.replace("MOD","M")
    dff["MODULO"] = dff["MODULO"].str.strip().str.replace(' ', '', regex=False)
    dff['FECHA DE PLANTACION'] = pd.to_datetime(
        dff['FECHA DE PLANTACION'].apply(parse_mixed_date), errors='coerce'
    ).dt.date



    dff["LOTE"] = dff["LOTE"].apply(format_lote)
    dff["LOTE"] = dff["LOTE"].str.replace(" -1","-1")
    dff["ID"] = dff["FUNDO"]+"_"+dff["LOTE"]
    dff = dff.drop(columns = ['ANO','SEM'])
    dff.columns = [col.capitalize() for col in dff.columns]
    dff = dff.rename(columns = {
        "Area actualizada":"Area",
        "N° de plantas":"Plantas Por Lote",
        "Plantas/ha":"Densidad",
        "Fecha de plantacion":"Fecha De Plantacion",
        "Id":"ID"
        }
    )
    dff["Fundo"] = dff["Fundo"].replace({"TARA FARM":"LAS BRISAS","CANYON":"EL POTRERO"})
    dff["MOD"] = dff["Modulo"].str.extract(r"(\d+)")[0].fillna("0").astype(int)
    dff["Fecha De Plantacion"] = pd.to_datetime(dff["Fecha De Plantacion"]).dt.date

    return dff

def cosecha_dataset_2():
    access_token = get_access_token()
    list_files = [
            "1. Cosecha Excelence Sur 2025 CAMPO San Jose I.xlsx",
            "2. Cosecha Excelence Sur 2025 CAMPO San Jose II LIS (1).xlsx",
            "5. Cosecha QBERRIES-CAMPAÑA-2025.xlsx",
            "3. Cosecha GAP - 2025.xlsx",
            "4. Cosecha TARA FARM - 2025.xlsx",
            "COSECHA FUNDO SAN PEDRO 2025 ACTUALIZADO.xlsx",
            "COSECHA CANYON BERRIES 2025 ACTUALIZADO.xlsx",
            #"REPORTE COSECHA LA COLINA ATLAS 2025..xlsx"

    ]

    files_check = [
        "1. Cosecha Excelence Sur 2025 CAMPO San Jose I.xlsx",
        "2. Cosecha Excelence Sur 2025 CAMPO San Jose II LIS (1).xlsx",
        "4. Cosecha TARA FARM - 2025.xlsx",
        "COSECHA FUNDO SAN PEDRO 2025 ACTUALIZADO.xlsx",
        "COSECHA CANYON BERRIES 2025 ACTUALIZADO.xlsx",
    ]
    files_skip = [
        "COSECHA FUNDO SAN PEDRO 2025 ACTUALIZADO.xlsx",
        "COSECHA CANYON BERRIES 2025 ACTUALIZADO.xlsx"
    ]
    data = pd.DataFrame()
    for file in list_files:
    #costo_laboral_diario= read_costo_laboral()
    #st.dataframe(costo_laboral_diario)
        
        if file in files_check:
            sheet_name = "KG PLANTA Y CAMPO "
            skip_rows = None
        
        elif file == "5. Cosecha QBERRIES-CAMPAÑA-2025.xlsx":
            sheet_name = "KG PLANTA Y CAMPO"
        elif file == " 3. Cosecha GAP - 2025.xlsx":
            sheet_name = "KG PLANTA Y CAMPO"
            
        if file in files_skip:
            skip_rows = 8
        else:
            skip_rows = None

        df = cosecha_datasets(access_token,file,sheet_name,skip_rows)
        
        df = df.loc[:, [c for c in df.columns if "UNNAMED" not in str(c).strip().upper()]]
        df.columns = [str(c).strip().upper() for c in df.columns]
        df.columns = df.columns.str.replace('\r', '').str.replace('\n', '')
        df = df.rename(columns={
            "PACKIG": "PACKING",
            "KG+CAMPO":"KG+DESCARTE(KILOS BRUTOS)",
            "KG EXPORT": "KG ENVIADOS A PLANTA",
            #"KG+DESCARTE(KILOS BRUTOS)":"KG+DESCARTE\r\n(KILOS BRUTOS)"

        })
        #st.dataframe(df)
        data = pd.concat([data,df],axis=0)
    return data

def clean_cosecha_2():
    data = cosecha_dataset_2()
    data = data[data["FUNDO"].notna()]
    data = data.drop(["COLUMNA1","DEVOLUCIÓN","JARRAS DE DESCARTE","KG PLANTA","MES11","MES","AÑO"], axis=1)
    data["KG CAMPO"] = data["KG CAMPO"].fillna(0)
    data["KG CAMPO"] = data["KG CAMPO"].replace(" ",0)
    data["KG CAMPO"] = data["KG CAMPO"].replace("",0)
    data["KG CAMPO"] = data["KG CAMPO"].astype(float)
    data["KG DESCARTE"] = data["KG DESCARTE"].fillna(0)
    data["KG ENVIADOS A PLANTA"] = data["KG ENVIADOS A PLANTA"].fillna(0)
    data["KG ENVIADOS A PLANTA"] = data["KG ENVIADOS A PLANTA"].replace(" ",0)
    data["KG ENVIADOS A PLANTA"] = data["KG ENVIADOS A PLANTA"].replace("",0)
    data["KG ENVIADOS A PLANTA"] = data["KG ENVIADOS A PLANTA"].astype(float)
    data["KG/JARRA  CAMPO"] = data["KG/JARRA  CAMPO"].fillna(0)
    data["KG/JARRA EXPO"] = data["KG/JARRA EXPO"].fillna(0)
    data["KG/JARRA"] = data["KG/JARRA"].fillna(0)
    data["JARRA"] =data["JARRA"].fillna(0)
    data["KG+DESCARTE(KILOS BRUTOS)"] =data["KG+DESCARTE(KILOS BRUTOS)"].fillna(0)
    data["FECHA"] = pd.to_datetime(data["FECHA"]).dt.date
    data = data.rename(columns={"KG+DESCARTE(KILOS BRUTOS)": "KILOS BRUTOS"})
    data["FUNDO"] = data["FUNDO"].replace({"QBERRIES":"LICAPA","GAP":"GAP BERRIES","CANYON BERRIES":"EL POTRERO"})
    return data


def kissflow_apl_nutricionales(access_token):
    data = listar_archivos_en_carpeta_compartida(
        access_token,
        "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        "01KM43WT4N7QGTHMP56JDY6Q3IMPH4SJGN"
    )
    file_general = get_download_url_by_name(data, "Registros Kissflow - Aplicaciones nutricionales General.xlsx")
    file_qberries = get_download_url_by_name(data, "Registros Kissflow - Aplicaciones nutricionales Qberries.xlsx")
    general_ap_df = pd.read_excel(file_general)
    qberries_ap_df = pd.read_excel(file_qberries)

    return  pd.concat([general_ap_df,qberries_ap_df],axis=0)

def kissflow_drenajes(access_token):
    data = listar_archivos_en_carpeta_compartida(
        access_token,
        "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        "01KM43WT4N7QGTHMP56JDY6Q3IMPH4SJGN"
    )
    file_general = get_download_url_by_name(data, "Registros Kissflow - Drenajes general.xlsx")
    file_qberries = get_download_url_by_name(data, "Registros Kissflow - Drenajes Qberries.xlsx")
    general_ap_df = pd.read_excel(file_general)
    qberries_ap_df = pd.read_excel(file_qberries)

    return  pd.concat([general_ap_df,qberries_ap_df],axis=0)

def kissflow_muestras(access_token):
    data = listar_archivos_en_carpeta_compartida(
        access_token,
        "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        "01KM43WT4N7QGTHMP56JDY6Q3IMPH4SJGN"
    )
    file_general = get_download_url_by_name(data, "Registros Kissflow - PH y CE general.xlsx")
    file_qberries = get_download_url_by_name(data, "Registros Kissflow - PH y CE Qberries.xlsx")
    general_ap_df = pd.read_excel(file_general)
    qberries_ap_df = pd.read_excel(file_qberries)

    return  pd.concat([general_ap_df,qberries_ap_df],axis=0)



def transform_kissflow_nutricionales():
    access_token = get_access_token()
    df = kissflow_apl_nutricionales(access_token)
    df = df[(df["FECHA"]!="Text data")&(df["FECHA"].notna())]
    df["CAMPAÑA"] = df["CAMPAÑA"].str.upper()
    df["FECHA"] = pd.to_datetime(df["FECHA"], format="%Y-%m-%d").dt.date
    df["TURNO"] = df["TURNO"].str[1:].astype(int)
    df["LOTE"] = df["LOTE"].str.replace("LT","LOTE ")
    df["OBSERVACION"] = df["OBSERVACION"].fillna("-")
    df["OBJETIVO"] = df["OBJETIVO"].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    df["OBJETIVO"] = df["OBJETIVO"].str.upper()
    df.columns = [str(c).strip().upper() for c in df.columns]
    apl_nutri_historico_df = pd.read_parquet("./data/APLICACIONES NUTRICIONALES.parquet")
    dff = pd.concat([apl_nutri_historico_df,df])
    return dff


def transform_kissflow_drenajes():
    access_token = get_access_token()
    df = kissflow_drenajes(access_token)
    df.columns = [str(c).strip().upper() for c in df.columns]
    df = df[df["FECHA"].notna()]
    df["MODULO"] = df["MODULO"].fillna("MX")
    df["MODULO"] = df["MODULO"].str.strip()
    df["MODULO"] = df["MODULO"].str.upper()
    df["TURNO"] = df["TURNO"].str[1:].astype(int)
    df["TURNO"] = df["TURNO"].astype(str)
    df["UBICACIÓN"] = df["UBICACIÓN"].fillna("NO ESPECIFICADO")
    df["UBICACIÓN"] = df["UBICACIÓN"].str.strip()
    df["UBICACIÓN"] = df["UBICACIÓN"].str.upper()

    cols = ['FECHA','FUNDO', 'MODULO', 'TURNO', 'VARIEDAD',
        'UBICACIÓN']
    cols_float = ['ETO MM/DIA', 'LÁMINA(MM)', 'REPOSICIÓN MM',
        'VOL DREN.1', 'VOL DREN. 2', 'VOLUMEN AFORO', '% DRENAJE REAL',
        '% MÍNIMO', '% MÁXIMO','VALVULA']
    for col in cols_float:
        df[col] = df[col].astype(float)

    df['AÑO'] = df['AÑO'].astype(int)
    df['SEMANA'] = df['SEMANA'].astype(int)
    drenajes_historico_df = pd.read_parquet("./data/DRENAJES.parquet")
    dff = pd.concat([drenajes_historico_df,df])
    return dff

def transform_kissflow_meq():
    access_token = get_access_token()
    data = listar_archivos_en_carpeta_compartida(
        access_token,
        "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        "01KM43WT4N7QGTHMP56JDY6Q3IMPH4SJGN"
    )
    df = pd.read_excel(get_download_url_by_name(
        data, 
        "Registros Kissflow - MEQ.xlsx"),
        #sheet_name = "REGISTRO",
    )
    df.columns = [str(c).strip().upper() for c in df.columns]
    dff = pd.read_excel("./data/MEQ - ZONA SUR.xlsx")
    dff = dff.drop(columns = ["Unnamed: 0"])
    dff.columns = [str(c).strip().upper() for c in dff.columns]
    meq_dff = pd.concat([df,dff],axis=0)
    meq_dff["FUNDO"] = meq_dff["FUNDO"].str.strip()
    meq_dff["FUNDO"] = meq_dff["FUNDO"].replace("GAP","GAP BERRIES")
    meq_dff["FECHA"] = pd.to_datetime(meq_dff["FECHA"],errors='coerce').dt.date
    cols_numeric = ['NH4', 'NO3', 'K', 'CA', 'MG', 'SO4', 'H2PO4','% NH4', '%NO3', 'N/K', 'CA/MG']
    for col in cols_numeric:
        meq_dff[col] = meq_dff[col].fillna(0)
        meq_dff[col] = pd.to_numeric(meq_dff[col],errors='coerce')
    
    meq_dff = meq_dff.melt(id_vars=["FUNDO","FECHA"],value_vars=cols_numeric,var_name="Atributo",value_name="Valor")
    meq_dff["Atributo"] = meq_dff["Atributo"].replace({"CA":"Ca","MG":"Mg"})
    meq_dff = meq_dff[meq_dff["Valor"]>0]
    return meq_dff

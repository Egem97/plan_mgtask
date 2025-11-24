import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import os
import re
from utils.utils import read_excel_resilient, read_excel_fast, format_lote, parse_mixed_date, lacolina_transform, clean_turno, sanitize_for_parquet

from utils.helpers import get_download_url_by_name
from utils.get_api import listar_archivos_en_carpeta_compartida
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
    data = listar_archivos_en_carpeta_compartida(
        access_token,
        "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        "01KM43WT22U7MUWEHXNRBZBLP6734DCWJO"
    )
    url_excel_1 = get_download_url_by_name(data, "REGISTRO GENERAL DE RIEGO Y FERTIRRIEGO.xlsx")
    #url_excel_2 = get_download_url_by_name(data, "REGISTRO APLICACIONES NUTRICIONALES-FUNDO QBERRIES.xlsx")
    return read_excel_fast(url_excel_1, sheet_name="BASE DE DATOS FERTIRRIEGO ", skiprows=1)



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


def cosecha_datasets(acess_token,name_excel,preferred_sheet,skip_rows):
    data = listar_archivos_en_carpeta_compartida(
        access_token,
        "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        "01KM43WT3E3DVAFNF4ZRGJS66GDVM3ZRMO"
    )
    url_excel_1 = get_download_url_by_name(data, name_excel)
    
    return read_excel_fast(url_excel_1,sheet_name=preferred_sheet, skiprows=skip_rows)#, sheet_name="DATA DRENAJE", skiprows=1


def aplicativoNutricional(access_token):
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
    return df


def FertiRiego(access_token):
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
    fertiriego_df["FUNDO"] = fertiriego_df["FUNDO"].replace({"QBERRIES":"LICAPA","CANYON BERRIES":"EL POTRERO"})
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
def parametros_campo(access_token):
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
    return df_general

#df_muestras = parametros_campo()
#st.write(df_muestras.shape)
#st.dataframe(df_muestras)
#print(df_muestras.info())
#print(df_muestras["FUNDO"].unique())
def drenaje_campo(access_token):
    df = drenaje_data(access_token)
    df = df.loc[:, [c for c in df.columns if not str(c).strip().upper().startswith("UNNAMED")]]
    df.columns = [str(c).strip().upper() for c in df.columns]
    df = df[df["FECHA"].notna()]
    df = df.drop(columns=["COLUMNA1","COLUMNA2"])
    df["FUNDO"] = df["FUNDO"].fillna("NO ESPECIFICADO")
    df["FUNDO"] = df["FUNDO"].str.strip()
    df["FUNDO"] = df["FUNDO"].str.upper()
    df["FUNDO"] = df["FUNDO"].replace({"CANYON BERRIES":"EL POTRERO","QBERRIES":"LICAPA"})

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
    for c in cols_num_drenaje:
        df[c] = df[c].fillna(0)
        df[c] = df[c].astype(float)

    df["FECHA"] = pd.to_datetime(df["FECHA"]).dt.date
    return df


def data_cosecha(access_token):
    #path_cosecha = r"C:\Users\EdwardoGiampiereEnri\OneDrive - ALZA PERU GROUP S.A.C\Archivos de Andy Rodriguez - INTELIGENCIA DE NEGOCIOS\DATA BI\AGRICOLA\PRODUCCION"
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
        #df = read_excel_resilient(full_path, sheet_name=preferred_sheet, skiprows=skip_rows)#,dtype={"FECHA":"str"}
        df = cosecha_datasets(token,file,preferred_sheet,skip_rows)
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
        print(file)
        print(df.shape)
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
    print(data["MODULO"].unique())
    data["TURNO"] = data["TURNO"].fillna(0)
    cols_numeric = ['HA REAL', 'KG/HA REAL', 'HA', 'JORNAL', 'JABAS', 'JARRAS',
        'KILOS BRUTOS', 'KILOS /HA', 'JARRAS/JR', 'KG/JR', 'DESCARTE']
    for col in cols_numeric:
        data[col] = data[col].fillna(0)
    data["FECHA"] = pd.to_datetime(data["FECHA"]).dt.date
    return data

#1. Cosecha Excelence Sur 2025 CAMPO San Jose I.xlsx


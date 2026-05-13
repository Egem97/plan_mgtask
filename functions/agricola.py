import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import os
import re
from utils.utils import *
from utils.get_kiss import fetch_all_kissflow
from utils.helpers import get_download_url_by_name,get_month_name,calcular_semana_anclada_enero
from utils.get_api import listar_archivos_en_carpeta_compartida
from utils.utils import read_excel_fast
from pandas.core.frame import DataFrame
from utils.get_token import get_access_token

def inf_plantacion():
    data = listar_archivos_en_carpeta_compartida(
        get_access_token(),
        "b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR",
        "01XOBWFSDI34HN5SD7HBHZRUBPX3457IPQ"
    )
    url_parquet = get_download_url_by_name(data, "INFORME PLANTAS.parquet")
    inf_pl_df = pd.read_parquet(url_parquet)
    inf_pl_df = inf_pl_df.groupby(['Fundo', 'Modulo', 'Turno'])[["Area"]].sum().reset_index()#,'Variedad'
    inf_pl_df["Turno"] = inf_pl_df["Turno"].astype(int)
    inf_pl_df["Turno"] = inf_pl_df["Turno"].astype(str)
    inf_pl_df.columns = [str(c).strip().upper() for c in inf_pl_df.columns]
    return inf_pl_df


def inf_plantacion_variedad():
    data = listar_archivos_en_carpeta_compartida(
        get_access_token(),
        "b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR",
        "01XOBWFSDI34HN5SD7HBHZRUBPX3457IPQ"
    )
    url_parquet = get_download_url_by_name(data, "INFORME PLANTAS.parquet")
    inf_pl_df = pd.read_parquet(url_parquet)
    inf_pl_df = inf_pl_df.groupby(['Fundo', 'Modulo', 'Turno','Variedad'])[["Area"]].sum().reset_index()#,'Variedad'
    inf_pl_df["Turno"] = inf_pl_df["Turno"].astype(int)
    #inf_pl_df["Turno"] = inf_pl_df["Turno"].astype(str)
    inf_pl_df["Modulo"] = inf_pl_df["Modulo"].str[1:]
    inf_pl_df["Variedad"] = inf_pl_df["Variedad"].replace("MÁGICA","MAGICA")
    inf_pl_df = inf_pl_df.drop(columns=["Area"])
    
    inf_pl_df.columns = [str(c).strip().upper() for c in inf_pl_df.columns]
    return inf_pl_df


def inf_plantacion_variedad_lote():
    data = listar_archivos_en_carpeta_compartida(
        get_access_token(),
        "b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR",
        "01XOBWFSDI34HN5SD7HBHZRUBPX3457IPQ"
    )
    url_parquet = get_download_url_by_name(data, "INFORME PLANTAS.parquet")
    inf_pl_df = pd.read_parquet(url_parquet)
    inf_pl_df = inf_pl_df.groupby(['Fundo', 'Modulo', 'Turno','Lote','Variedad'])[["Area"]].sum().reset_index()#,'Variedad'
    inf_pl_df["Turno"] = inf_pl_df["Turno"].astype(int)
    #inf_pl_df["Turno"] = inf_pl_df["Turno"].astype(str)
    inf_pl_df["Modulo"] = inf_pl_df["Modulo"].str[1:]
    inf_pl_df["Variedad"] = inf_pl_df["Variedad"].replace("MÁGICA","MAGICA")
    inf_pl_df = inf_pl_df.drop(columns=["Area"])
    
    inf_pl_df.columns = [str(c).strip().upper() for c in inf_pl_df.columns]
    return inf_pl_df

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


def poligonos():
    data = listar_archivos_en_carpeta_compartida(
        get_access_token(),
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


    dff["LOTE"] = dff["LOTE"].str.replace("-A","A")
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
    general_ap_df = pd.read_excel(file_general,dtype={'Agua Programada (M3)':str})
    qberries_ap_df = pd.read_excel(file_qberries,dtype={'Agua Programada (M3)':str})

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


def kissflow_riego_fertirriego(access_token):
    data = listar_archivos_en_carpeta_compartida(
        access_token,
        "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        "01KM43WT4N7QGTHMP56JDY6Q3IMPH4SJGN"
    )
    file_general = get_download_url_by_name(data, "Registros Kissflow - Riego y fertirriego.xlsx")
    file_qberries = get_download_url_by_name(data, "Registros Kissflow - Riego y fertirriego Qberries.xlsx")
    df = pd.concat([pd.read_excel(file_general),pd.read_excel(file_qberries)],axis=0)
    df = df.drop(columns = ["AÑO","SEMANA","TOTAL ()"])#
    df = df[df["FECHA"]>='2026-02-03']
    df.columns = [str(c).strip().upper() for c in df.columns]
    df = df[df["FECHA"].notna()]
    df["TURNO ORIGINAL"] = df["TURNO"]
    df["TURNO ORIGINAL"] = df["TURNO ORIGINAL"].fillna("0")
    df["TURNO ORIGINAL"] = df["TURNO ORIGINAL"].astype(str)
    df["TURNO"] = df["TURNO"].fillna(99)

    df["MES"] = df["MES"].str.upper()
    df["MODULO"] = df["MODULO"].fillna("X")
    df["MODULO"] = df["MODULO"].astype(str)
    df["MODULO"] = 'M' +df["MODULO"].astype(str)
    df["MODULO"] = df["MODULO"].str.replace(".0","")
    df["TURNO"] = df["TURNO"].apply(clean_turno).astype(int)
    df["TURNO"] = df["TURNO"].astype(int)

    df["FASE"] = df["FASE"].fillna("NO ESPECIFICADO")
    df["FASE"] = df["FASE"].str.strip()
    df["EQUIPO"] = df["EQUIPO"].fillna("NO ESPECIFICADO")
    df["EQUIPO"] = df["EQUIPO"].str.strip()
    df["SUPERVISOR"] = df["SUPERVISOR"].fillna("NO ESPECIFICADO")
    df["SUPERVISOR"] = df["SUPERVISOR"].str.strip()
    df["DESCRIPCIÓN"] = df["DESCRIPCIÓN"].fillna("NO ESPECIFICADO")   
    df["DESCRIPCIÓN"] = df["DESCRIPCIÓN"].str.strip()

    df["AREA"] = df["AREA"].fillna("0")
    df["AREA"] = df["AREA"].astype(str)
    df["AREA"] = df["AREA"].str.strip()
    df["AREA"] = df["AREA"].replace("","0")
    df["AREA"] = df["AREA"].replace("-","0")
    df["AREA"] = df["AREA"].astype(float)

    df["VARIEDAD"] = df["VARIEDAD"].fillna("NO ESPECIFICADO")
    df["VARIEDAD"] = df["VARIEDAD"].str.strip()
    df["VARIEDAD"] = df["VARIEDAD"].str.upper()
    df["VARIEDAD"] = df["VARIEDAD"].replace("SEKOYA","SEKOYA POP")
    df["FUNDO"] = df["FUNDO"].fillna("NO ESPECIFICADO")
    df["FUNDO"] = df["FUNDO"].str.strip()
    df["FUNDO"] = df["FUNDO"].str.upper()
    df["FUNDO"] = df["FUNDO"].replace({"QBERRIES":"LICAPA","CANYON BERRIES":"EL POTRERO","QBERRIES II":"LICAPA II"})
    df["FECHA"] = pd.to_datetime(df["FECHA"]).dt.date

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
            df[c] = df[c].fillna("0")
            df[c] = df[c].astype(str)
            df[c] = df[c].str.strip()
            df[c] = df[c].replace("","0")
            df[c] = df[c].replace("-","0")
            df[c] = df[c].astype(float)
    df = df.drop(columns=["TURNO ORIGINAL","TOTAL (MIN)"])
    df["TURNO"] = df["TURNO"].astype(str)
    mask_gap = df["FUNDO"] == "GAP BERRIES"
    df.loc[mask_gap & df["TURNO"].isin(["1","2","3","4"]), "TURNO"] = "1"
    df.loc[mask_gap & df["TURNO"].isin(["5","6","7","8"]), "TURNO"] = "2"
    df.loc[mask_gap & df["TURNO"].isin(["9","10","11"]), "TURNO"] = "3"
    df.loc[mask_gap & df["TURNO"].isin(["12","13","14"]), "TURNO"] = "4"
    df.loc[mask_gap & df["TURNO"].isin(["15","16","17"]), "TURNO"] = "5"
    df.loc[mask_gap & df["TURNO"].isin(["18","19","20"]), "TURNO"] = "6"
    df.loc[mask_gap & df["TURNO"].isin(["21","22","23"]), "TURNO"] = "7"
    df.loc[mask_gap & df["TURNO"].isin(["24","25","26"]), "TURNO"] = "8"
    df.loc[mask_gap & df["TURNO"].isin(["27","28"]), "TURNO"] = "9"
    df.loc[mask_gap & df["TURNO"].isin(["29","30","31"]), "TURNO"] = "10"

    mask_colina = df["FUNDO"] == "LA COLINA"
    df.loc[mask_colina & df["TURNO"].isin(["1","2","3","4","5","6","7","8","9","10"]), "TURNO"] = "1"
    return df



def transform_kissflow_nutricionales():
    access_token = get_access_token()
    df = kissflow_apl_nutricionales(access_token)
    df = df[(df["FECHA"]!="Text data")&(df["FECHA"].notna())]

    #####
    dff = fetch_all_kissflow("RIE_03_1_BD")
    dff = dff.rename(columns={
        "FECHA_DE_REGISTRO":"FECHA",
        "Fecha_de_creacion":"FECHA_DE_CREACION",
        "AREA_PROGRAMADA":"ÁREA PROGRAMADA ",
        "AREA_EJECUTADA":"ÁREA EJECUATADA",
        "INGREDIENTE_ACTIVO":"INGREDIENTE ACTIVO",
        "VIA_DE_APLICACION":"VIA DE APLICACIÓN",
        "DOSISHA":"DOSIS/HA",
        "CANTIDAD_TOTAL":"CANTIDAD TOTAL ",
        "OBSERVACIONES":"OBSERVACION"
        
    })

    
    dff["CANTIDAD TOTAL "] = dff["CANTIDAD TOTAL "].astype(float)
    dff["ÁREA PROGRAMADA "] = dff["ÁREA PROGRAMADA "].astype(float)
    dff["CAMPAÑA"] = "CAMPAÑA 2026"
    dff["FECHA"] = pd.to_datetime(dff["FECHA"], errors="coerce")
    dff["AÑO"] = dff["FECHA"].dt.year
    dff["MES"] = dff["FECHA"].dt.month
    dff["MES"] = dff["MES"].map(get_month_name)
    dff["SEMANA"] = dff["FECHA"].apply(calcular_semana_anclada_enero).astype("Int64")
    #dff["FECHA_DE_REGISTRO"] = pd.to_datetime(dff["FECHA_DE_REGISTRO"], errors="coerce").dt.date
    dff["FECHA_DE_CREACION"] = pd.to_datetime(dff["FECHA_DE_CREACION"], errors="coerce")
    partes = dff["TURNO_MODULO"].astype(str).str.extract(
        r"^L(?P<LOTE>\S+)\s*-\s*T(?P<TURNO>\d+)\s*-\s*M(?P<MODULO>\d+)"
    )
    lote_limpio = partes["LOTE"].str.replace(r"^0+", "", regex=True)
    dff["LOTE"] = lote_limpio.mask(lote_limpio == "", "0")
    dff["TURNO"] = partes["TURNO"].astype("Int64")
    dff["MODULO"] = partes["MODULO"]
    
   
    dff = dff.drop(columns=["TURNO_MODULO"])
    inf_pl_df = inf_plantacion_variedad_lote()
    inf_pl_df["LOTE"] = (
        inf_pl_df["LOTE"]
        .astype(str)
        .str.replace(r"(?i)\bLOTE\b", "", regex=True)
        .str.strip()
        .str.replace(r"^0+", "", regex=True)
    )
    inf_pl_df["LOTE"] = inf_pl_df["LOTE"].mask(inf_pl_df["LOTE"] == "", "0")

    dff = pd.merge(dff, inf_pl_df, on=["FUNDO","MODULO","TURNO","LOTE"], how="left")
    dff = dff[dff["FECHA_DE_CREACION"]>='2026-05-13']
    dff["FECHA_DE_CREACION"] = pd.to_datetime(dff["FECHA_DE_CREACION"], errors="coerce").dt.date
    ######
    df = pd.concat([df, dff],axis=0, ignore_index=True)
    
    df["CAMPAÑA"] = df["CAMPAÑA"].str.upper()
    df["FECHA"] = pd.to_datetime(df["FECHA"], format="%Y-%m-%d").dt.date
    #df["TURNO"] = df["TURNO"].str[1:].astype(int)
    #df["LOTE"] = df["LOTE"].str.replace("LT","LOTE ")
    df["LOTE"] = df["LOTE"].astype(str)
    df["LOTE"] = df["LOTE"].apply(format_lote)
    df["MODULO"] = df["MODULO"].fillna(0)
    df["MODULO"] = "M"+df["MODULO"].astype(int).astype(str)
    
    df["OBSERVACION"] = df["OBSERVACION"].fillna("-")
    df["OBSERVACION"] = df["OBSERVACION"].astype(str)
    df["OBJETIVO"] = df["OBJETIVO"].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    df["OBJETIVO"] = df["OBJETIVO"].str.upper()

    df["OBJETIVO"] = df["OBJETIVO"].fillna("-")
    df["VOLUMEN"] = df["VOLUMEN"].fillna(0)
    df["GRUPO"] = df["GRUPO"].replace("BIOESTIMULANTE","BIOESTIMULATES")
    df.columns = [str(c).strip().upper() for c in df.columns]
    df["FUNDO"] = df["FUNDO"].fillna("NO ESPECIFICADO")
    df["FUNDO"] = df["FUNDO"].str.upper()
    df["FUNDO"] = df["FUNDO"].str.strip()
    df["FUNDO"] = df["FUNDO"].replace("QBERRIES","LICAPA")
    df["FUNDO"] = df["FUNDO"].replace("QBERRIES II","LICAPA II")
    df["FUNDO"] = df["FUNDO"].replace("CANYON BERRIES","EL POTRERO")
   
    apl_nutri_historico_df = pd.read_parquet("./data/APLICACIONES NUTRICIONALES.parquet")
    apl_nutri_historico_df["FUNDO"] = apl_nutri_historico_df["FUNDO"].replace("CANYON BERRIES","EL POTRERO")
    df = pd.concat([apl_nutri_historico_df,df])
    df["FECHA_DE_CREACION"] =df["FECHA_DE_CREACION"].fillna("2026-01-01")
    df["FECHA_DE_CREACION"] = pd.to_datetime(df["FECHA_DE_CREACION"], errors="coerce").dt.date 
    df["ÁREA EJECUATADA"] = df["ÁREA EJECUATADA"].astype(float)
    df["DOSIS/HA"] = df["DOSIS/HA"].astype(float)
    df["VOLUMEN"] = df["VOLUMEN"].astype(float)
    return df

def data_drenaje_kissflow():
    dff = fetch_all_kissflow("RIE4_1_BD")
    dff = dff.rename(columns={
            "FECHA_DE_REGISTRO":"FECHA",
            "NH4":"TURNOMODULO",
            "UBICACION":"UBICACIÓN",
            "Agua_Programada_M3":"AGUA PROGRAMADA (M3)",
            "ETO":"ETO MM/DIA",
            "LAMINA":"LÁMINA(MM)",
            "REPOSICION":"REPOSICIÓN MM",
            "CANT_DRENAJE_1":"VOL DREN.1",
            "CANT_DREANJE_2":"VOL DREN. 2",
            "MINIMO":"% MÍNIMO",
            "MAXIMO":"% MÁXIMO",
            "CALCULO_DE_DRENAJE_REAL":"% DRENAJE REAL",
            "CANTIDAD_AFORO":"VOLUMEN AFORO"

    })
    dff["% MÍNIMO"] = pd.to_numeric(
        dff["% MÍNIMO"].astype(str).str.replace("%", "", regex=False).str.strip(),
        errors="coerce",
    ) / 100
    dff["% MÁXIMO"] = pd.to_numeric(
        dff["% MÁXIMO"].astype(str).str.replace("%", "", regex=False).str.strip(),
        errors="coerce",
    ) / 100
    dff["VOL DREN.1"] = dff["VOL DREN.1"].fillna(0)
    dff["VOL DREN.1"] = dff["VOL DREN.1"].astype(float)
    dff["VOL DREN. 2"] = dff["VOL DREN. 2"].fillna(0)
    dff["VOL DREN. 2"] = dff["VOL DREN. 2"].astype(float)
    dff["VOLUMEN AFORO"] = dff["VOLUMEN AFORO"].fillna(0)
    dff["VOLUMEN AFORO"] = dff["VOLUMEN AFORO"].astype(float)
    #dff["% DRENAJE REAL"] = dff["% DRENAJE REAL"].fillna(0)
    #dff["% DRENAJE REAL"] = dff["% DRENAJE REAL"]/100
    dff["FECHA"] = pd.to_datetime(dff["FECHA"], errors="coerce")
    dff["AÑO"] = dff["FECHA"].dt.year
    #dff["MES"] = dff["FECHA"].dt.month
    #dff["MES"] = dff["MES"].map(get_month_name)
    dff["SEMANA"] = dff["FECHA"].apply(calcular_semana_anclada_enero).astype("Int64")
    dff["FECHA_DE_CREACION"] = pd.to_datetime(dff["FECHA_DE_CREACION"], errors="coerce")
    partes = dff["TURNOMODULO"].astype(str).str.extract(r"T(?P<TURNO>\d+)-M(?P<MODULO>\d+)")
    dff["TURNO"] = partes["TURNO"].astype("Int64")
    dff["MODULO"] =partes["MODULO"]
    dff = dff.drop(columns=["TURNOMODULO","DE_REPOSICION","DRENAJE_REAL"])
    dff = dff[dff["FECHA_DE_CREACION"]>='2026-05-13']
    inf_pl_df =inf_plantacion_variedad()
    dff = pd.merge(dff, inf_pl_df, on=["FUNDO","MODULO","TURNO"], how="left")
    return dff

def transform_kissflow_drenajes(drenaje_kissflow):
    access_token = get_access_token()
    df = kissflow_drenajes(access_token)
    df.columns = [str(c).strip().upper() for c in df.columns]
    df = df[df["FECHA"].notna()]

    ####
    
    df= pd.concat([df,drenaje_kissflow])

    ###############
    #df.to_excel("prueba_aguaprogramada2.xlsx",index=False)
    df["MODULO"] = df["MODULO"].fillna(0)
    df["MODULO"] = df["MODULO"].astype(int)
    df["MODULO"] = "M"+df["MODULO"].astype(str)
    df["TURNO"] = df["TURNO"].fillna(0)
    df["TURNO"] = df["TURNO"].astype(str)
    df["FUNDO"] = df["FUNDO"].replace("CANYON BERRIES","EL POTRERO")
    #df["TURNO"] = df["TURNO"].astype(str)
    df["UBICACIÓN"] = df["UBICACIÓN"].fillna("NO ESPECIFICADO")
    df["UBICACIÓN"] = df["UBICACIÓN"].str.strip()
    df["UBICACIÓN"] = df["UBICACIÓN"].str.upper()
    df["VALVULA"] = df["VALVULA"].fillna(0)
    df["VALVULA"] = df["VALVULA"].astype(int)
    df["VARIEDAD"] = df["VARIEDAD"].fillna("NO ESPECIFICADO")
    df["VARIEDAD"] = df["VARIEDAD"].str.strip()
    df["VARIEDAD"] = df["VARIEDAD"].str.upper()
    df = df[(df["VOL DREN.1"].notna())&(df["VOL DREN. 2"].notna())]
    df = df[(df["VOL DREN.1"]!=0)&(df["VOL DREN. 2"]!=0)]
    
    cols_float = ['ETO MM/DIA', 'LÁMINA(MM)', 'REPOSICIÓN MM',
        '% DRENAJE REAL',
        '% MÍNIMO', '% MÁXIMO','VALVULA','AGUA PROGRAMADA (M3)']
    for col in cols_float:
        df[col] = df[col].astype(float)
    df["FECHA"] = pd.to_datetime(df["FECHA"]).dt.date
    df['AÑO'] = df['AÑO'].astype(int)
    df['SEMANA'] = df['SEMANA'].astype(int)
    #print(df.columns)
    df = df[(df["VOL DREN.1"].notna())&(df["VOL DREN. 2"].notna())]
    df = df[(df["VOL DREN.1"]!=0)&(df["VOL DREN. 2"]!=0)]
    drenajes_historico_df = pd.read_parquet("./data/DRENAJE.parquet")
    dff = pd.concat([drenajes_historico_df,df])
    dff["FECHA_DE_CREACION"] =dff["FECHA_DE_CREACION"].fillna("2026-01-01")
    dff["FECHA_DE_CREACION"] = pd.to_datetime(dff["FECHA_DE_CREACION"], errors="coerce").dt.date
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
    dff["MODULO"] = "1"
    meq_dff = pd.concat([df,dff],axis=0)
    #
    
    #####
    df_kissflow = fetch_all_kissflow("RIE06_1_BD")
    df_kissflow = df_kissflow.rename(columns={
            "FECHA_DE_REGISTRO":"FECHA",
            "OONH4":"% NH4",
            "OONO3":"%NO3",
            "NK":"N/K",
            "CAMG":"CA/MG",

    })
    df_kissflow["MODULO"] = df_kissflow["MODULO"].fillna("M0")
    df_kissflow["MODULO"] = df_kissflow["MODULO"].str[1:]
    df_kissflow["MODULO"] = df_kissflow["MODULO"].astype(int)
    df_kissflow["FECHA_DE_CREACION"] = pd.to_datetime(df_kissflow["FECHA_DE_CREACION"], errors="coerce")
    df_kissflow = df_kissflow[df_kissflow["FECHA_DE_CREACION"]>='2026-05-13']
    
    ####
    meq_dff = pd.concat([meq_dff,df_kissflow])
    meq_dff["FUNDO"] = meq_dff["FUNDO"].str.strip()
    meq_dff["FUNDO"] = meq_dff["FUNDO"].replace("GAP","GAP BERRIES")
    meq_dff["FUNDO"] = meq_dff["FUNDO"].replace("CANYON BERRIES","EL POTRERO")
    meq_dff["FECHA"] = pd.to_datetime(meq_dff["FECHA"],errors='coerce')
    meq_dff["MODULO"] = meq_dff["MODULO"].fillna(0)
    meq_dff["MODULO"] = meq_dff["MODULO"].astype(str)
    meq_dff["MODULO"] = "M"+meq_dff["MODULO"]
    meq_dff["MODULO"] = meq_dff["MODULO"].str.replace(".0","")
    cols_numeric = ['NH4', 'NO3', 'K', 'CA', 'MG', 'SO4', 'H2PO4','% NH4', '%NO3', 'N/K', 'CA/MG']
    for col in cols_numeric:
        meq_dff[col] = meq_dff[col].fillna(0)
        meq_dff[col] = pd.to_numeric(meq_dff[col],errors='coerce')
    
    meq_dff = meq_dff.melt(id_vars=["FUNDO","FECHA","MODULO"],value_vars=cols_numeric,var_name="Atributo",value_name="Valor")
    meq_dff["Atributo"] = meq_dff["Atributo"].replace({"CA":"Ca","MG":"Mg"})
    meq_dff = meq_dff[meq_dff["Valor"]>0]
    meq_dff["SEMANA"] = meq_dff["FECHA"].dt.isocalendar().week
    meq_dff["FECHA"] = meq_dff["FECHA"].dt.date
    qberries = meq_dff[meq_dff["FUNDO"].isin(["LICAPA","LICAPA II"])]
    meq_dff = meq_dff[~meq_dff["FUNDO"].isin(["LICAPA","LICAPA II"])]
    #meq_dff = meq_dff.groupby(["FUNDO","MODULO","Atributo","SEMANA"]).agg(
    #    {"Valor": "sum","FECHA":"min"}
    #).reset_index()

    qberries = qberries.groupby(["FUNDO","MODULO","Atributo","SEMANA"]).agg(
        {"Valor": "mean","FECHA":"min"}
    ).reset_index()
    qberries["Valor"] = qberries["Valor"].round(2)
    meq_dff = pd.concat([meq_dff,qberries],axis=0)
    meq_dff = meq_dff.reset_index()
    meq_dff = meq_dff.drop(columns = ["index"])
    return meq_dff


def completed_kissflow_muestras():
    access_token = get_access_token()
    df = kissflow_muestras(access_token)
    #df["FECHA_DE_CREACION"] = datetime(2026,1,1)
    ####
    dff = fetch_all_kissflow("RIE_1_1_BD")
    dff["FECHA_DE_REGISTRO"] = pd.to_datetime(dff["FECHA_DE_REGISTRO"], errors="coerce")
    dff["AÑO"] = dff["FECHA_DE_REGISTRO"].dt.year
    dff["MES"] = dff["FECHA_DE_REGISTRO"].dt.month
    
    dff["MES"] = dff["MES"].map(get_month_name)
    dff["SEMANA"] = dff["FECHA_DE_REGISTRO"].apply(calcular_semana_anclada_enero).astype("Int64")
    dff["FECHA_DE_REGISTRO"] = pd.to_datetime(dff["FECHA_DE_REGISTRO"], errors="coerce").dt.date
    
    dff["FECHA_DE_CREACION"] = pd.to_datetime(dff["FECHA_DE_CREACION"], errors="coerce")#.dt.date
    
    partes = dff["TURNOMODULO"].astype(str).str.extract(r"T(?P<TURNO>\d+)-M(?P<MODULO>\d+)")
    dff["TURNO"] = partes["TURNO"].astype("Int64")
    dff["MODULO"] =partes["MODULO"]
    dff = dff.rename(columns={"FECHA_DE_REGISTRO":"FECHA","TIPO_DE_MUESTRA":"TIPO DE MUESTRA","PARAMETROS":"PARAMETRO"})
    dff = dff.drop(columns=["TURNOMODULO"])
    dff = dff[dff["FECHA_DE_CREACION"]>='2026-05-13']
    dff["FECHA_DE_CREACION"] = pd.to_datetime(dff["FECHA_DE_CREACION"], errors="coerce").dt.date
    inf_pl_df =inf_plantacion_variedad()
    dff = pd.merge(dff, inf_pl_df, on=["FUNDO","MODULO","TURNO"], how="left")
    print(dff.shape)
    #########
    df = pd.concat([df,dff])

    df["FUNDO"] = df["FUNDO"].fillna("NO ESPECIFICADO")
    df["FUNDO"] = df["FUNDO"].str.strip()
    df["TIPO DE MUESTRA"] = df["TIPO DE MUESTRA"].fillna("NO ESPECIFICADO")
    df["TIPO DE MUESTRA"] = df["TIPO DE MUESTRA"].str.strip()
    df["VARIEDAD"] = df["VARIEDAD"].fillna("NO ESPECIFICADO")
    df["VARIEDAD"] = df["VARIEDAD"].str.strip()
    df["VARIEDAD"] = df["VARIEDAD"].replace("MÁGICA","MAGICA")
    df["FUNDO"] = df["FUNDO"].fillna("NO ESPECIFICADO")
    df["FUNDO"] = df["FUNDO"].str.strip()
    df["FUNDO"] = df["FUNDO"].str.upper()
    df["FUNDO"] = df["FUNDO"].replace("CANYON BERRIES","EL POTRERO")
    df["MES"] = df["MES"].fillna("NO ESPECIFICADO")
    df["MES"] = df["MES"].str.strip()
    df["MES"] = df["MES"].str.upper()
    df["MES"] = df["MES"].str.upper()
    df["PROFUNDIDAD"] = df["PROFUNDIDAD"].fillna("0")
    df["PROFUNDIDAD"] = (
            df["PROFUNDIDAD"].astype(str)
            .str.extract(r"(\d+(?:\.\d+)?)", expand=False)
            .fillna("0")
            .astype(float)
            .astype(int)
    )
    df["MODULO"] = df["MODULO"].fillna(0)
    df["MODULO"] = df["MODULO"].astype(int)
    df["MODULO"] = "M"+df["MODULO"].astype(str)
    df["TURNO"] = df["TURNO"].fillna(0)
    df["TURNO"] = df["TURNO"].astype(int)
    df["TURNO"] = df["TURNO"].astype(str)
    mask_colina_ = df["FUNDO"] == "LA COLINA"
    df.loc[mask_colina_ & df["TURNO"].isin(["1","2","3","4","5","6","7","8","9","10"]), "TURNO"] = "1"
        
    df["PARAMETRO"] = df["PARAMETRO"].fillna("XX")
    df["PARAMETRO"] = df["PARAMETRO"].str.upper()
    df["LECTURA"] = df["LECTURA"].fillna(0)
    df["L MIN"] = df["L MIN"].fillna(0)
    df["L MAX"] = df["L MAX"].fillna(0)
    df["FECHA"] = pd.to_datetime(df["FECHA"]).dt.date
    df["TIPO DE MUESTRA"] = df["TIPO DE MUESTRA"].str.upper()
    df["INDEX"] = df["TIPO DE MUESTRA"].replace({
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
            "NO ESPECIFICADO":99,
            "OSMOSIS 2A":21,
            "OSMOSIS 2 A":21,
            "OSMOSIS 2B":22,
            "OSMOSIS 2 B":22,
            "POZO 1":23,
            "POZO 5":24,
            "POZO 2":15
    })

    hist_df = pd.read_parquet("./data/MUESTRAS.parquet")

    dataframe = pd.concat([df,hist_df])
    dataframe = dataframe[dataframe["TIPO DE MUESTRA"]!="SOL. FIBRA COCO"]
    dataframe = dataframe[dataframe["TIPO DE MUESTRA"]!="GOTERO"]
    dataframe["FECHA_DE_CREACION"] =dataframe["FECHA_DE_CREACION"].fillna("2026-01-01")
    dataframe["FECHA_DE_CREACION"] = pd.to_datetime(dataframe["FECHA_DE_CREACION"], errors="coerce").dt.date
    
    return dataframe    


def transform_kissflow_insumos(drenaje_kissflow):
    df = kissflow_riego_fertirriego(get_access_token())
    fertiriego_historico_df = pd.read_parquet("./data/INSUMOS.parquet")
    fertiriego_historico_df = fertiriego_historico_df[pd.to_datetime(fertiriego_historico_df["FECHA"])<"2026-02-03"]
    drenaje_df = transform_kissflow_drenajes_agua(drenaje_kissflow)
    drenaje_df = drenaje_df[drenaje_df["AGUA PROGRAMADA (M3)"].notna()]
    columns_drenajes_insumos = [
    'FECHA','FUNDO','MODULO', 'TURNO', 'VARIEDAD','AGUA PROGRAMADA (M3)','ETO MM/DIA',
    'LÁMINA(MM)','REPOSICIÓN MM'
    ]
    drenaje_df = drenaje_df[columns_drenajes_insumos]
    drenaje_df["CAMPAÑA"] = "CAMPAÑA 2026"
    drenaje_df["FASE"] = "FASE 2"
    drenaje_df["EQUIPO"] = "NO ESPECIFICADO"
    drenaje_df["SUPERVISOR"] = "NO ESPECIFICADO"
    drenaje_df["DESCRIPCIÓN"] = "NO ESPECIFICADO"
    drenaje_df = drenaje_df.rename(columns = {
            'ETO MM/DIA':'ETO',
            'LÁMINA(MM)':'LAMINA (MM)',
            'REPOSICIÓN MM':'% REPOSICION'
    })
    dff = pd.concat([df,fertiriego_historico_df,drenaje_df],axis=0)
    dff["VARIEDAD"] = dff["VARIEDAD"].replace({
        "MÁGICA": "MAGICA",
        
    })
    dff["TURNO"] = dff["TURNO"].astype(int)
    dff["TURNO"] = dff["TURNO"].astype(str)
    dff = dff[['CAMPAÑA', 'FASE', 'MES', 'FECHA', 'EQUIPO', 'SUPERVISOR', 'FUNDO',
       'DESCRIPCIÓN', 'MODULO', 'TURNO', 'VARIEDAD',
       'LECTURA DE HIDROMETRO INICIAL', 'LECTURA DE HIDROMETRO FINAL',
       'AGUA PROGRAMADA (M3)', 'ETO', 'LAMINA (MM)', '% REPOSICION', 'TANQ. 1',
       'TANQ. 2', 'TANQ. 3', 'TANQ. 4', 'TANQ. 5', 'TANQ. 6', 'TANQ.7']]
    inf_pl_df = inf_plantacion()
    dff = pd.merge(dff, inf_pl_df, on=["FUNDO","MODULO","TURNO"], how="left")
    dff["AREA"] = dff["AREA"].fillna(1)
    return dff

def transform_kissflow_drenajes_agua(drenaje_kissflow):
    access_token = get_access_token()
    df = kissflow_drenajes(access_token)
    df.columns = [str(c).strip().upper() for c in df.columns]
    df = df[df["FECHA"].notna()]
    ###
    df = pd.concat([df,drenaje_kissflow])
    ##
    
    df["MODULO"] = df["MODULO"].fillna(0)
    df["MODULO"] = df["MODULO"].astype(int)
    df["MODULO"] = "M"+df["MODULO"].astype(str)
    df["TURNO"] = df["TURNO"].fillna(0)
    df["TURNO"] = df["TURNO"].astype(str)
    df["FUNDO"] = df["FUNDO"].replace("CANYON BERRIES","EL POTRERO")
    #df["TURNO"] = df["TURNO"].astype(str)
    df["UBICACIÓN"] = df["UBICACIÓN"].fillna("NO ESPECIFICADO")
    df["UBICACIÓN"] = df["UBICACIÓN"].str.strip()
    df["UBICACIÓN"] = df["UBICACIÓN"].str.upper()
    df["VALVULA"] = df["VALVULA"].fillna(0)
    df["VALVULA"] = df["VALVULA"].astype(int)
    df["VARIEDAD"] = df["VARIEDAD"].fillna("NO ESPECIFICADO")
    df["VARIEDAD"] = df["VARIEDAD"].str.strip()
    df["VARIEDAD"] = df["VARIEDAD"].str.upper()
    #df = df[(df["VOL DREN.1"].notna())&(df["VOL DREN. 2"].notna())]
    #df = df[(df["VOL DREN.1"]!=0)&(df["VOL DREN. 2"]!=0)]
    #df.to_excel("prueba_aguaprogramada.xlsx",index=False)
    cols_float = ['ETO MM/DIA', 'LÁMINA(MM)', 'REPOSICIÓN MM',
        '% DRENAJE REAL',
        '% MÍNIMO', '% MÁXIMO','VALVULA','AGUA PROGRAMADA (M3)']
    for col in cols_float:
        df[col] = df[col].astype(float)
    df["FECHA"] = pd.to_datetime(df["FECHA"]).dt.date
    df['AÑO'] = df['AÑO'].astype(int)
    df['SEMANA'] = df['SEMANA'].fillna(0)
    df['SEMANA'] = df['SEMANA'].astype(int)
    #print(df.columns)
    #df = df[(df["VOL DREN.1"].notna())&(df["VOL DREN. 2"].notna())]
    #df = df[(df["VOL DREN.1"]!=0)&(df["VOL DREN. 2"]!=0)]
    drenajes_historico_df = pd.read_parquet("./data/DRENAJE.parquet")
    dff = pd.concat([drenajes_historico_df,df])
    return dff


def proy_licapa_2026(access_token):
    data = listar_archivos_en_carpeta_compartida(
        access_token,
        "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        "01KM43WT4X24C427242BC3VTLLENU7KEWG"
    )
    #url_excel_1 = get_download_url_by_name(data, "REGISTRO GENERAL DE APLICACIONES NUTRICIONALES.xlsx")
    url_excel = get_download_url_by_name(data, "Kg LLICAPA I-II 2026.xlsx")
    #PPT KG26
    licapa_bd =read_excel_fast(url_excel, sheet_name="BD")
    licapa_bd.columns = [str(c).strip().upper() for c in licapa_bd.columns]
    licapa_bd["FUNDO"] = licapa_bd["FUNDO"].str.strip()
    licapa_bd["MODULO"] = licapa_bd["MODULO"].str.strip()
    licapa_bd["VARIEDAD"] = licapa_bd["VARIEDAD"].str.strip()
    licapa_bd["VARIABLE"] = licapa_bd["VARIABLE"].str.strip()
    licapa_bd["VARIABLE"] = licapa_bd["VARIABLE"].str.upper()
    licapa_bd["KILOS"] = licapa_bd["KILOS"].fillna(0)
    licapa_bd["INDICADOR"] = licapa_bd["FUNDO"].astype(str)+"-"+licapa_bd["MODULO"].astype(str)+" "+licapa_bd["VARIABLE"].astype(str)
    print(licapa_bd.columns)
    licapa_ppt =read_excel_fast(url_excel, sheet_name="PPT KG26")
    licapa_ppt.columns = [str(c).strip().upper() for c in licapa_ppt.columns]
    licapa_ppt["FUNDO"] = licapa_ppt["FUNDO"].str.strip()
    licapa_ppt["MODULO"] = licapa_ppt["MODULO"].str.strip()
    licapa_ppt["VARIEDAD"] = licapa_ppt["VARIEDAD"].str.strip()
    licapa_ppt["KG/PPTO 26"] = licapa_ppt["KG/PPTO 26"].fillna(0)
    licapa_ppt["KG/PPTO 26"] = licapa_ppt["KG/PPTO 26"].astype(float)
    print(licapa_ppt.columns)
    return licapa_bd,licapa_ppt

def proy_all_2026(access_token):
    data = listar_archivos_en_carpeta_compartida(
        access_token,
        "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        "01KM43WT4X24C427242BC3VTLLENU7KEWG"
    )
    #url_excel_1 = get_download_url_by_name(data, "REGISTRO GENERAL DE APLICACIONES NUTRICIONALES.xlsx")
    url_excel = get_download_url_by_name(data, "Kg SJ GAP TARA SP CA-2026.xlsx")
    #PPT KG26
    all_df =read_excel_fast(url_excel, sheet_name="BD")
    all_df.columns = [str(c).strip().upper() for c in all_df.columns]
    all_df["VARIABLE"] = all_df["VARIABLE"].str.upper()
    all_df["KILOS"] = all_df["KILOS"].fillna(0)
    all_df["KILOS"] = all_df["KILOS"].astype(float)
    all_df["FUNDO"] = all_df["FUNDO"].str.strip()
    all_df["VARIABLE"] = all_df["VARIABLE"].str.strip()
    all_df["INDICADOR"] = all_df["FUNDO"]+" "+all_df["VARIABLE"]
    print(all_df["INDICADOR"].unique())
    print(len(all_df["INDICADOR"].unique()))
    all_ppt =read_excel_fast(url_excel, sheet_name="PPTO 26")
    all_ppt.columns = [str(c).strip().upper() for c in all_ppt.columns]
    all_ppt["KG/PPTO 26"] = all_ppt["KG/PPTO 26"].fillna(0)
    all_ppt["KG/PPTO 26"] = all_ppt["KG/PPTO 26"].astype(float)
    print(all_ppt["FUNDO"].unique())
    print(len(all_ppt["FUNDO"].unique()))
    return all_df,all_ppt

def proy_2026():
    token = get_access_token()
    proy_all_df,ppt_all_dff = proy_all_2026(token)
    proy_licapa_df,ppt_licapa_df = proy_licapa_2026(token)
    proy_df = pd.concat([proy_all_df,proy_licapa_df],axis=0)
    proy_df["MODULO"] = proy_df["MODULO"].fillna("I")
    mask_sanjose2 = proy_df["FUNDO"] == "SAN JOSE II"
    proy_df.loc[mask_sanjose2 & (proy_df["MODULO"]=="I"), "MODULO"] = "II"
    proy_df["FUNDO"] = proy_df["FUNDO"].replace({
        'SAN JOSE I':'SAN JOSE',
        'TARA FARM':'LAS BRISAS',
        'CANYON':'EL POTRERO',
        'LICAPA I':'LICAPA'
    })
    ppt_df = pd.concat([ppt_all_dff,ppt_licapa_df],axis=0)
    ppt_df["MODULO"] = ppt_df["MODULO"].fillna("I")
    maskppt_sanjose2 = ppt_df["FUNDO"] == "SAN JOSE II"
    ppt_df.loc[maskppt_sanjose2 & (ppt_df["MODULO"]=="I"), "MODULO"] = "II"
    ppt_df["VARIEDAD"] = ppt_df["VARIEDAD"].fillna("SEKOYA POP")
    mask_ppt_canyon_madeira = (ppt_df["FUNDO"] == "CANYON MADEIRA")
    mask_ppt_magica_magica = (ppt_df["FUNDO"] == "CANYON MAGIC")
    ppt_df.loc[mask_ppt_canyon_madeira & (ppt_df["VARIEDAD"]=="SEKOYA POP"), "VARIEDAD"] = "MADEIRA"
    ppt_df.loc[mask_ppt_magica_magica & (ppt_df["VARIEDAD"]=="SEKOYA POP"), "VARIEDAD"] = "MAGICA"
    ppt_df["FUNDO"] = ppt_df["FUNDO"].str.strip()
    ppt_df["FUNDO"] = ppt_df["FUNDO"].replace({
        'SAN JOSE I':'SAN JOSE',
        #'TARA FARM':'LAS BRISAS',
        #'CANYON':'EL POTRERO',
        'LICAPA I':'LICAPA',
        #'CANYON MADEIRA':'EL POTRERO',
        #'CANYON MAGIC':'EL POTRERO',
    })
    proy_df["KILOS"] = proy_df["KILOS"].replace({0:None})
    ppt_df["KG/PPTO 26"] = ppt_df["KG/PPTO 26"].replace({0:None})
    return proy_df,ppt_df

def proyecciones_2026():
    FILES = {
    "GAP": "PROYECCIONES 2026- GAP.xlsx",
    "SJ":  "PROYECCIONES 2026- SJ 1.xlsx",
    "SP":  "PROYECCIONES 2026- SP.xlsx",
    "2025": "PROYECCIONES 2024 -2025 SEM 04 SJ1,SJ2-TARA - GAP.xlsx",
    "QBERRIES":"PROYECCIONES 2026- QBERRIES.xlsx",
    "CY":"PROYECCIONES 2026 CANYON.xlsx"
    
    }

    FUNDO_REPLACE = {
        "GAP BERRIES 2026": "GAP BERRIES",
        "SAN JOSE I 2026":  "SAN JOSE",
        "SAN JOSE II 2026": "SAN JOSE II",
        "SAN PEDRO 2026":   "SAN PEDRO",
        "GAP BERRIES 2024":"GAP BERRIES",
        "GAP BERRIES 2025":"GAP BERRIES",
        "SAN JOSE I 2024":"SAN JOSE",
        "SAN JOSE I 2025":"SAN JOSE",
        "SAN JOSE II 2025":"SAN JOSE II",
        "SJ-I  2025":"SAN JOSE",
        "TARA FARM I 2025":"LAS BRISAS"
    }


    NUMERIC_COLS = [
        "N° BROTES", "YEMA  HINCHADA", "YEMA ABIERTA", "YEMA INACTIVA ",
        "YEMA HINCHADA/BROTE", "YEMA ABIERTA/BROTE", "YEMA/ BROTE ",
        "MILLONES DE BOTONES", "BOTON", "FLOR", "CUAJA",
        "E1 (verde)", "E2 ", "E3", "E4 (50 %-70%)", "E5  (Maduro)",
        "ORGANOS", "CARGADORES", "FRUTOS", "KG",
        "YEMA  RP KG", "YEMA AB KG ", "YEMA IN KG ",
        "BOTON KG", "FOR KG", "CUAJA KG",
        "E1 KG", "E2 KG", "E3 KG", "E4 KG", "E5 KG",
        "AREA","YEMA INACTIVA ","YEMA AB KG ", "YEMA IN KG "
    ]

    STRING_COLS = ["VARIEDAD", "FUNDO", "BOTON FLORAL", "MODULO"]


    def clean_df(df: pd.DataFrame) -> pd.DataFrame:
        # Strip column names
        df.columns = [str(c).strip().upper() for c in df.columns]

        # Remove unnamed columns
        df = df.loc[:, [c for c in df.columns if not c.startswith("UNNAMED")]]

        # Drop rows where FECHA is null (fully empty rows)
        df = df[df["FECHA"].notna()].copy()

        # --- String columns ---
        for col in STRING_COLS:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.upper()

        # Clean FUNDO
        df["FUNDO"] = df["FUNDO"].replace(FUNDO_REPLACE)
        # Fallback: strip year suffix pattern like " 2026" or " I 2026"
        df["FUNDO"] = df["FUNDO"].str.replace(r"\s+(I|II)?\s*2026$", "", regex=True).str.strip()

        # Clean BOTON FLORAL
        #df["BOTON FLORAL"] = df["BOTON FLORAL"].replace(BOTON_FLORAL_REPLACE)
        #df["BOTON FLORAL"] = df["BOTON FLORAL"].str.replace(r"\s+(I|II)?\s*2026$", "", regex=True).str.strip()

        # Clean MODULO (normalize to roman numeral string)
        df["MODULO"] = df["MODULO"].astype(str).str.strip().str.upper()
        df["MODULO"] = df["MODULO"].replace({"1": "I", "2": "II", "3": "III"})

        # TURNO and LOTE as int then string (consistent with agricola.py pattern)
        df["TURNO"] = df["TURNO"].fillna(0).astype(int).astype(str)
        df["LOTE"]  = df["LOTE"].fillna(0).astype(str)#.astype(int)

        # --- Date ---
        df["FECHA"] = pd.to_datetime(df["FECHA"]).dt.date

        # --- Numeric columns: fill NaN with 0 ---
        # Normalize names before matching (df.columns already stripped+uppercased)
        for col in NUMERIC_COLS:
            col_clean = col.strip().upper()
            if col_clean in df.columns:
                df[col_clean] = pd.to_numeric(df[col_clean], errors="coerce").fillna(0)
        # Catch-all: any remaining numeric-dtype columns with NaN → 0
        num_cols = df.select_dtypes(include="number").columns
        df[num_cols] = df[num_cols].fillna(0)

        # AÑO and SEMANA as int
        df["AÑO"]    = df["AÑO"].fillna(2026).astype(int)
        df["SEMANA"] = df["SEMANA"].fillna(0).astype(int)

        # Add CAMPAÑA column for traceability
        df.insert(0, "CAMPAÑA", "CAMPAÑA 2026")

        # Final column order
        col_order = [
            "CAMPAÑA", "SEMANA", "AÑO", "FECHA",
            "VARIEDAD", "FUNDO", "BOTON FLORAL", "MODULO", "TURNO", "LOTE", "AREA",
            "N° BROTES",
            "YEMA  HINCHADA", "YEMA ABIERTA", "YEMA INACTIVA ",
            "YEMA HINCHADA/BROTE", "YEMA ABIERTA/BROTE", "YEMA/ BROTE ",
            "MILLONES DE BOTONES",
            "BOTON", "FLOR", "CUAJA",
            "E1 (verde)", "E2 ", "E3", "E4 (50 %-70%)", "E5  (Maduro)",
            "ORGANOS", "CARGADORES", "FRUTOS", "KG",
            "YEMA  RP KG", "YEMA AB KG ", "YEMA IN KG ",
            "BOTON KG", "FOR KG", "CUAJA KG",
            "E1 KG", "E2 KG", "E3 KG", "E4 KG", "E5 KG",
        ]
        ordered = [c for c in col_order if c in df.columns]
        extras  = [c for c in df.columns if c not in col_order]
        df = df[ordered + extras]

        return df.reset_index(drop=True)
    access_token = get_access_token()
    dfs = []
    for key, filename in FILES.items():
        print(filename)
        data = listar_archivos_en_carpeta_compartida(
            access_token,
            "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
            "01KM43WT4BO3PJI4NFQFDYPGOO3EXBRQSN"
        )
        
        url_excel = get_download_url_by_name(data, filename)
        
        
        raw = pd.read_excel(url_excel, sheet_name="BASE")
        
        cleaned = clean_df(raw)
        if "PROYECCIONES 2024 -2025 SEM 04 SJ1,SJ2-TARA - GAP":
            cleaned["CAMPAÑA"] = "CAMPAÑA 2025"
        cleaned["ORIGEN"] = key
        dfs.append(cleaned)
        #print(f"[{key}] {len(raw)} filas → {len(cleaned)} filas limpias | FUNDO: {cleaned['FUNDO'].unique()}")

    df_all = pd.concat(dfs, axis=0, ignore_index=True)
    print(f"\nTotal consolidado: {len(df_all)} filas")
    print(f"FUNDOS: {sorted(df_all['FUNDO'].unique())}")
    return df_all

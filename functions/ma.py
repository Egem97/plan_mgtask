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


def read_ma():
    access_token = get_access_token()
    files = [
        "MA BIG BERRIES.xlsx",#["MA BIG BERRIES","xlsx"]
        "MA CANYON.xlsx",
        "MA EXCELLENCE.xlsx",
        "MA GAP.xlsx",
        "MA QBERRIES.xlsx",
        "MA TARA.xlsx"
    ]   
    data_general = pd.DataFrame()
    for file in files:
        #data = listar_archivos_en_carpeta_compartida(
        #    access_token,
        #    "b!DKrRhqg3EES4zcUVZUdhr281sFZAlBZDuFVNPqXRguBl81P5QY7KRpUL2n3RaODo",
        #    "01PNBE7BCWAEJYZDUI5BCYKSDJEFC37UGC"
        #)
        
        #url_excel= get_download_url_by_name(data, file)
        #st.write(file)
        path_ma = r"C:\Users\EdwardoGiampiereEnri\OneDrive - ALZA PERU GROUP S.A.C\Archivos de Ana Huaco - COSTOS - POWER BI\MAYOR ANALITICO"
        if file == "MA QBERRIES.xlsx":
            df = read_excel_fast(path_ma+"/"+file,sheet_name="Quelen")
        else:
            df = read_excel_fast(path_ma+"/"+file)

        df = df.loc[:, [c for c in df.columns if not str(c).strip().upper().startswith("UNNAMED")]]
        df.columns = [str(c).strip().upper() for c in df.columns]
        df["EMPRESA"]=file.split(".")[0]
        df = df.rename(columns={"TIPO PPTO":"TIPO"})
        #st.dataframe(df)
        
        #df = clean_costo_laboral(df)
        #import streamlit as st
        #st.write(file)
        data_general = pd.concat([data_general,df],axis=0)
    data_general = data_general.drop(["AÑO","SEMANA","TIPO1","MES"],axis=1)    
    data_general = data_general.drop([
        'VOUCHER CONTABLE','NUMERO OPERACION','DOCUMENTO REFERENCIA',
        'SOLES CARGO','SOLES ABONO', 'SOLES SALDO','DÓLARES SALDO','IDCCOSTO',
        'DOC. ORIGEN MONEDA','DESCRIPCIÓN MONEDA','MACRO PARTIDA','MACRO PARTIDA II'
    ],axis=1)
    data_general = data_general[data_general["FECHA"].notna()]
    data_general["FECHA"] = pd.to_datetime(data_general["FECHA"],format="%d/%m/%Y")
    data_general["CUENTA"] = data_general["CUENTA"].fillna("-")
    data_general["CUENTA"] = data_general["CUENTA"].str.strip()
    data_general["CUENTA"] = data_general["CUENTA"].astype("string")
    data_general["NOMBRE CTA. CONTABLE"] = data_general["NOMBRE CTA. CONTABLE"].str.strip()
    data_general["NOMBRE CTA. CONTABLE"] = data_general["NOMBRE CTA. CONTABLE"].astype("string")
    data_general["GLOSA"] = data_general["GLOSA"].fillna("-")
    data_general["GLOSA"] = data_general["GLOSA"].str.strip()
    data_general["GLOSA"] = data_general["GLOSA"].astype("string")
    data_general["CÓDIGO CLIENTE/PROVEEDOR"] = data_general["CÓDIGO CLIENTE/PROVEEDOR"].fillna("-")
    data_general["CÓDIGO CLIENTE/PROVEEDOR"] = data_general["CÓDIGO CLIENTE/PROVEEDOR"].str.strip()
    data_general["CÓDIGO CLIENTE/PROVEEDOR"] = data_general["CÓDIGO CLIENTE/PROVEEDOR"].astype("string")
    data_general["RAZÓN SOCIAL"] = data_general["RAZÓN SOCIAL"].fillna("-")
    data_general["RAZÓN SOCIAL"] = data_general["RAZÓN SOCIAL"].str.strip()
    data_general["RAZÓN SOCIAL"] = data_general["RAZÓN SOCIAL"].astype("string")
    data_general["COD. PROYECTO"] = data_general["COD. PROYECTO"].fillna("O000")
    data_general["COD. PROYECTO"] = data_general["COD. PROYECTO"].str.strip()
    data_general["COD. PROYECTO"] = data_general["COD. PROYECTO"].astype("string")
    data_general["DESCRIPCIÓN PROYECTO"] = data_general["DESCRIPCIÓN PROYECTO"].fillna("-")
    data_general["DESCRIPCIÓN PROYECTO"] = data_general["DESCRIPCIÓN PROYECTO"].str.strip()
    data_general["DESCRIPCIÓN PROYECTO"] = data_general["DESCRIPCIÓN PROYECTO"].astype("string")
    data_general["DESCRIPCIÓN PROYECTO"] = data_general["DESCRIPCIÓN PROYECTO"].apply(quitar_tildes)

    data_general["COD. ACTIVIDAD"] = data_general["COD. ACTIVIDAD"].fillna("X1X")
    data_general["COD. ACTIVIDAD"] = data_general["COD. ACTIVIDAD"].str.strip()
    data_general["COD. ACTIVIDAD"] = data_general["COD. ACTIVIDAD"].astype("string")
    data_general["DESCRIPCIÓN ACTIVIDAD"] = data_general["DESCRIPCIÓN ACTIVIDAD"].fillna("-")
    data_general["DESCRIPCIÓN ACTIVIDAD"] = data_general["DESCRIPCIÓN ACTIVIDAD"].str.strip()
    data_general["DESCRIPCIÓN ACTIVIDAD"] = data_general["DESCRIPCIÓN ACTIVIDAD"].astype("string")

    data_general["CAMPAÑA"] = data_general["CAMPAÑA"].fillna("CAMPAÑA 2025")
    data_general["CAMPAÑA"] = data_general["CAMPAÑA"].str.strip()
    data_general["CAMPAÑA"] = data_general["CAMPAÑA"].str.upper()
    
    data_general["CAMPAÑA"] = data_general["CAMPAÑA"].astype("string")
    data_general["SEDE"] = data_general["SEDE"].fillna(data_general["EMPRESA"])
    data_general["SEDE"] = data_general["SEDE"].replace(
        {
        "MA BIG BERRIES": "LA COLINA", 
        "MA CANYON": "EL POTRERO",
        "MA GAP" : "GAP BERRIES",
        "MA QBERRIES": "LICAPA",
        "MA TARA": "LAS BRISAS"
        }
    )
    mask_excellence = data_general["EMPRESA"] == "MA EXCELLENCE"
    data_general = data_general[~(mask_excellence & data_general["SEDE"].isin(["RVR", "SRI"]))]
    mask_excellence = data_general["EMPRESA"] == "MA EXCELLENCE"
    data_general.loc[mask_excellence, "SEDE"] = (
        data_general.loc[mask_excellence, "SEDE"]
            .replace({
                "CAPEX ALAMBRADO SP": "SAN PEDRO",
                "MEJORA SP": "SAN PEDRO",
                "MEJORAS SJ": "SAN JOSE",
                "SAN PEDRO MEJORA COSECHA": "SAN PEDRO",
            })
    )
    #Fecha_final
    
    #data_general["Año Campaña"]=data_general["CAMPAÑA"].str[8:].astype(int)
    #data_general["Año Fecha"]=data_general["FECHA"].dt.year
    #data_general["Fecha_final"] = data_general.apply(lambda x: fecha_final(x["FECHA"],x["Año Campaña"],x["Año Fecha"]),axis=1)
    #data_general = data_general.drop(columns = ["Año Campaña","Año Fecha"])
    data_general["EXTRAE"] = data_general["EXTRAE"].fillna("-")
    data_general["EXTRAE"] = data_general["EXTRAE"].str.strip()
    data_general["EXTRAE"] = data_general["EXTRAE"].astype("string")
    data_general["Nombre_Presupuesto"] = data_general["EXTRAE"].replace({

        "EC": "CAPEX",
        "EO": "OPEX",
        "O": "OPEX"
    })
    data_general = data_general.rename(columns={
        "CUENTA":"idcuenta",
        "NOMBRE CTA. CONTABLE":"nombre_cta",
        "FECHA":"fecha",
        "GLOSA":"glosa",
        "DÓLARES CARGO":"cargo_mex",
        "DÓLARES ABONO":"abono_mex",
        "CÓDIGO CLIENTE/PROVEEDOR":"idclieprov",
        "RAZÓN SOCIAL":"razonsocial",
        "COD. PROYECTO":"idproyecto",
        "DESCRIPCIÓN PROYECTO":"proyecto",
        "COD. ACTIVIDAD":"idactividad",
        "DESCRIPCIÓN ACTIVIDAD":"actividad",
        "CAMPAÑA":"Campaña",
        "COSTO":"IMPORTE NETO MEX",
        
    })
    data_general = data_general[~data_general["SEDE"].isin(["RVR","SRI"])]
    data_general["Año"] = data_general["fecha"].dt.year
    data_general["MES"] = data_general["fecha"].dt.month.astype(str).str.zfill(2)
    data_general["periodo"] = data_general["Año"].astype(str)+data_general["MES"]
    data_general["fecha"] = pd.to_datetime(data_general["fecha"],format="%d/%m/%Y")
    return data_general
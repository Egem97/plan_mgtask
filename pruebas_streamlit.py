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
drive_id = "b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR"
folder_id = "01XOBWFSG7XR5BMRB6XNAJBU7Q7KXU7BO3"
def historico_agritracer(access_token):
        data = listar_archivos_en_carpeta_compartida(
            access_token,
            drive_id,
            folder_id
        )
        url_parquet = get_download_url_by_name(data, "AGRITRACER_GENERAL.parquet")
        #url_excel_2 = get_download_url_by_name(data, "REGISTRO APLICACIONES NUTRICIONALES-FUNDO QBERRIES.xlsx")
        return pd.read_parquet(url_parquet, engine="pyarrow")



from functions.costos import *
from functions.transporte import *

def datos_costo_laboral():
        data = listar_archivos_en_carpeta_compartida(
            get_access_token(),
            "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
            "01KM43WT4FS6JNXKKHRNCJZMAXLX56IOEQ"
        )
        url_= get_download_url_by_name(data, "COSTO_LABORAL.parquet")
        df = pd.read_parquet(url_)
        
        return df
    
def datos_transporte_personal():
        data = listar_archivos_en_carpeta_compartida(
            get_access_token(),
            "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
            "01KM43WT25M5RFQXKLAVHLRU2SMBZSTYEE"
        )
        url_excel_1 = get_download_url_by_name(data, "1. TRANSPORTE PERSONAL 2026.xlsx")
        df = read_excel_fast(url_excel_1,sheet_name="CUADRO RESUMEN")    
        
        return df 


def datos_tipo_cambio_():
        data = listar_archivos_en_carpeta_compartida(
            get_access_token(),
            "b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR",
            "01XOBWFSDI34HN5SD7HBHZRUBPX3457IPQ"
        )
        url_excel_1 = get_download_url_by_name(data, "TIPO DE CAMBIO.xlsx")
        df = read_excel_fast(url_excel_1)   
        df = df[["FECHA","PrecioVenta"]]    
        df = df.rename(columns={"PrecioVenta":"TIPO_CAMBIO"}) 
        return df 
def datos_transporte_kias():
        data = listar_archivos_en_carpeta_compartida(
            get_access_token(),
            "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
            "01KM43WT4M3PZTZVTV3VC3ORHLY6J4WI2X"
        )
        url_excel_1 = get_download_url_by_name(data, "APG TK - CONTROL DIARIO KIAS - 2026.xlsx")
        df = read_excel_fast(url_excel_1, sheet_name="BD")
        
        return df
    
def datos_costos_manual():
        data = listar_archivos_en_carpeta_compartida(
            get_access_token(),
            "b!SVsG7KxV3EShgK6A6P5nH8F5fySlAsRHnUuG-SGbK8wEoIXhn_g4T4PoBqMjb_xH",
            "016WMGJCUL63W2LEO4JVE2ABL7PVCROI4O"
        )
        url_excel_1 = get_download_url_by_name(data, "1. Costos Cosecha.xlsx")
        dataframe = pd.DataFrame()
        sheets = [
            "QBERRIES I","QBERRIES II MAGICA","QBERRIES II SEKOYA","SAN JOSE",
            "SAN JOSE II", "SAN PEDRO","TARA","GAP","COLINA","CANYON"
        ]
        for fundo in sheets:
            df = read_excel_fast(url_excel_1, sheet_name=fundo)
            df.columns = [strip_accents(c).strip().upper() for c in df.columns]
            df = df[df["FECHA"].notna()]
            df["FUNDO"] = fundo
            df["CAMPANA"] = df["CAMPANA"].str.strip()
            df = df[df["CAMPANA"]=="Campaña 2026"]
            dataframe = pd.concat([dataframe,df])
        return dataframe
        
        """
        qb2_magica_df = read_excel_fast(url_excel_1, sheet_name="QBERRIES II MAGICA")
        qb2_magica_df.columns = [strip_accents(c).strip().upper() for c in qb2_magica_df.columns]
        qb2_magica_df = qb2_magica_df[qb2_magica_df["FECHA"].notna()]
        qb2_magica_df["FUNDO"] = "QBERRIES II MAGICA"
        
        
        
        qb3_magica_df = read_excel_fast(url_excel_1, sheet_name="QBERRIES II SEKOYA")
        qb3_magica_df.columns = [strip_accents(c).strip().upper() for c in qb3_magica_df.columns]
        qb3_magica_df["FUNDO"] = "QBERRIES II SEKOYA"
        
        
        qb_df = read_excel_fast(url_excel_1, sheet_name="QBERRIES I")
        qb_df.columns = [strip_accents(c).strip().upper() for c in qb_df.columns]
        qb_df["FUNDO"] = "QBERRIES I"
        qb_df["CAMPANA"] = qb_df["CAMPANA"].str.strip()
        qb_df = qb_df[qb_df["CAMPANA"]=="Campaña 2026"]
        
        sanjose_df = read_excel_fast(url_excel_1, sheet_name="SAN JOSE")
        sanjose_df.columns = [strip_accents(c).strip().upper() for c in sanjose_df.columns]
        sanjose_df["FUNDO"] = "SAN JOSE"
        sanjose_df["CAMPANA"] = sanjose_df["CAMPANA"].str.strip()
        sanjose_df = sanjose_df[sanjose_df["CAMPANA"]=="Campaña 2026"]
        
        sanjose2_df = read_excel_fast(url_excel_1, sheet_name="SAN JOSE II")
        sanjose2_df.columns = [strip_accents(c).strip().upper() for c in sanjose2_df.columns]
        sanjose2_df["FUNDO"] = "SAN JOSE II"
        sanjose2_df["CAMPANA"] = sanjose2_df["CAMPANA"].str.strip()
        sanjose2_df = sanjose2_df[sanjose2_df["CAMPANA"]=="Campaña 2026"]
        
        sanpedro_df = read_excel_fast(url_excel_1, sheet_name="SAN PEDRO")
        sanpedro_df.columns = [strip_accents(c).strip().upper() for c in sanpedro_df.columns]
        sanpedro_df["FUNDO"] = "SAN PEDRO"
        sanpedro_df["CAMPANA"] = sanpedro_df["CAMPANA"].str.strip()
        sanpedro_df = sanpedro_df[sanpedro_df["CAMPANA"]=="Campaña 2026"]
        
        
        
        
        df = pd.concat([qb2_magica_df,qb3_magica_df,qb_df,sanjose_df,sanjose2_df])
        return df
        """

def datos_agritracer():
        data = listar_archivos_en_carpeta_compartida(
            get_access_token(),
            "b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR",
            "01XOBWFSG7XR5BMRB6XNAJBU7Q7KXU7BO3"
        )
        url_ = get_download_url_by_name(data, "AGRITRACER_GENERAL.parquet")
        df = pd.read_parquet(url_)

        return df

def datos_cosecha_1():
        data = listar_archivos_en_carpeta_compartida(
            get_access_token(),
            "b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR",
            "01XOBWFSDI34HN5SD7HBHZRUBPX3457IPQ"
        )
        url_ = get_download_url_by_name(data, "COSECHA CAMPO.parquet")
        df = pd.read_parquet(url_)

        return df

def datos_transporte_interno():
        data = listar_archivos_en_carpeta_compartida(
            get_access_token(),
            "b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR",
            "01XOBWFSDC2SPT2RFM3BGY6TJUHKMNQGOI"
        )
        url_ = get_download_url_by_name(data, "TRANSPORTE_CAMARAS.parquet")
        df = pd.read_parquet(url_)
        
        return df
######################################################################################
#df = pd.read_parquet("AGRITRACER_GENERAL.parquet")

def builder_agri_jr(agritracer_df):
    agritracer_df["SEMANA"] = agritracer_df["FECHA"].apply(calcular_semana_anclada_enero).astype("Int64")
    agritracer_df["FECHA"] = pd.to_datetime(agritracer_df["FECHA"], errors="coerce")
    agritracer_df = agritracer_df[agritracer_df["FECHA"]>="2026-01-01"]
    agritracer_df["YEAR"] = agritracer_df["FECHA"].dt.year
    agritracer_df["PERIODO SEMANA"] = agritracer_df["SEMANA"].astype(str)+"-"+agritracer_df["YEAR"].astype(str)
    
    agritracer_df.loc[(agritracer_df["FUNDO"] == "EL POTRERO") & (agritracer_df["VARIEDAD"] == "MAGICA"), "FUNDO"] = "CANYON MAGICA"
    agritracer_df.loc[(agritracer_df["FUNDO"] == "EL POTRERO") & (agritracer_df["VARIEDAD"] != "MAGICA"), "FUNDO"] = "CANYON MADEIRA"
    agritracer_df["FUNDO"] = agritracer_df["FUNDO"].replace({
        "LICAPA II":"QBERRIES II MAGICA",
        "LICAPA III":"QBERRIES II SEKOYA",
        "LICAPA":"QBERRIES I",
        "GAP BERRIES":"GAP",
    })
    
    
    
    agritracer_df = agritracer_df.groupby(["FUNDO","YEAR","FECHA","PERIODO SEMANA","SEMANA","ACTIVIDAD","PARTIDA PRESUPUESTARIA","MACRO PARTIDA","TRABAJADOR"])[["COPIA"]].count().reset_index()
    agritracer_df = agritracer_df.groupby(["FUNDO","YEAR","FECHA","SEMANA","PERIODO SEMANA","ACTIVIDAD","PARTIDA PRESUPUESTARIA","MACRO PARTIDA"])[["COPIA"]].count().reset_index()
    agritracer_df["PERIODO SEMANA"] = agritracer_df["PERIODO SEMANA"].astype(str)
    agritracer_df = agritracer_df.rename(columns={"COPIA":"JORNALES_AGRITRACER"})
    index_cols = ["FUNDO","YEAR","FECHA","PERIODO SEMANA","SEMANA"]

    pivot_df = agritracer_df.pivot_table(
        index=index_cols,
        columns="MACRO PARTIDA",
        values="JORNALES_AGRITRACER",
        aggfunc="sum",
        fill_value=0
    ).reset_index()
    pivot_df.columns.name = None
    macro_cols = [c for c in pivot_df.columns if c not in index_cols]
    pivot_df = pivot_df.rename(columns={c: f"{c}_JR" for c in macro_cols})

    mo_cosecha_df = (
        agritracer_df[agritracer_df["PARTIDA PRESUPUESTARIA"] == "MO COSECHA"]
        .groupby(index_cols)["JORNALES_AGRITRACER"].sum().reset_index()
        .rename(columns={"JORNALES_AGRITRACER": "MO COSECHA"})
    )
    cosecha_df = (
        agritracer_df[agritracer_df["ACTIVIDAD"] == "COSECHA"]
        .groupby(index_cols)["JORNALES_AGRITRACER"].sum().reset_index()
        .rename(columns={"JORNALES_AGRITRACER": "COSECHA"})
    )

    pivot_df = pivot_df.merge(mo_cosecha_df, on=index_cols, how="left").merge(cosecha_df, on=index_cols, how="left")
    pivot_df[["MO COSECHA", "COSECHA"]] = pivot_df[["MO COSECHA", "COSECHA"]].fillna(0)
    pivot_df = pivot_df.rename(columns = {"MO COSECHA":"MO COSECHA_JR","COSECHA":"COSECHA_JR"})
    return pivot_df

def builder_cosecha(df):
    df["FECHA"] = pd.to_datetime(df["FECHA"], errors="coerce")
    df["MERCADO"] = df["MERCADO"].fillna(df["PACKING"])
    df["MERCADO"] = df["MERCADO"].str.strip()
    #df["MERCADO"] = 
    df = df[df["FECHA"]>="2026-06-01"]
    df["FUNDO_"] = df["FUNDO_"].replace({
        'CANYON MANILA':"CANYON MADEIRA"
    })
    df = df.rename(columns = {"FUNDO":"FUNDO_ALTERNO"})
    df = df.rename(columns = {"FUNDO_":"FUNDO"})
    df = df.drop(columns = ["CAMPAÑA"])
    
    df = df.groupby(["FECHA","FUNDO","FUNDO_ALTERNO","MERCADO","PACKING"])[["HA","JORNAL","JABAS","JARRAS","KILOS BRUTOS","KILOS /HA","DESCARTE"]].sum().reset_index()
    

    df = df.rename(columns={c: f"{c}_COS" for c in ["HA","JORNAL","JABAS","JARRAS","KILOS BRUTOS","KILOS /HA","DESCARTE"]})
    return df

def builder_transporte_personal(df,tc):
    df = df[df["FECHA"]>='2026-06-01']
    cols_pivot = [
        '(S/) GAP COSECHA','(S/) SAN JOSE II COSECHA','(S/) SAN JOSE COSECHA','(S/) CANYON COSECHA','(S/) CANYON 2 COSECHA','(S/) SAN PEDRO COSECHA','(S/) BIG BERRIES COSECHA',
        '(S/) GOLDEN BERRIES COSECHA','(S/) QBERRIES COSECHA','(S/) QBERRIES 2 COSECHA','(S/) QBERRIES 3 COSECHA','(S/) TARA COSECHA'
    ]
    df = df[['SEMANA', 'FECHA']+cols_pivot]
    df = df.melt(
        id_vars=['SEMANA', 'FECHA'],
        value_vars=cols_pivot,
        var_name="FUNDO",
        value_name="MONTO (S/)",
    )
    
    df["FUNDO"] = df["FUNDO"].replace(
        {
        '(S/) GAP COSECHA':'GAP',
        '(S/) SAN JOSE II COSECHA':"SAN JOSE II",
        '(S/) SAN JOSE COSECHA':"SAN JOSE",
        '(S/) CANYON COSECHA':"CANYON MAGICA",
        '(S/) CANYON 2 COSECHA':"CANYON MADEIRA",
        '(S/) SAN PEDRO COSECHA':"SAN PEDRO",
        '(S/) BIG BERRIES COSECHA':"LA COLINA", 
        '(S/) GOLDEN BERRIES COSECHA':"LA COLINA",
        '(S/) QBERRIES COSECHA':"QBERRIES I",
        '(S/) QBERRIES 2 COSECHA':"QBERRIES II MAGICA",
        '(S/) QBERRIES 3 COSECHA':"QBERRIES II SEKOYA", 
        '(S/) TARA COSECHA' :"LAS BRISAS"
        }
    )
    
    df = df[df["MONTO (S/)"]>0]
    #canyon = df[df["FUNDO"] == "CANYON"].copy()
    #canyon_magica = canyon.copy()
    #canyon_magica["FUNDO"] = "CANYON MAGICA"
    #canyon_magica["MONTO (S/)"] = canyon_magica["MONTO (S/)"] * 0.81
    #canyon_madeira = canyon.copy()
    #canyon_madeira["FUNDO"] = "CANYON MADEIRA"
    #canyon_madeira["MONTO (S/)"] = canyon_madeira["MONTO (S/)"] * 0.19
    #df = pd.concat([df[df["FUNDO"] != "CANYON"], canyon_magica, canyon_madeira], ignore_index=True)
    df["FECHA"] = pd.to_datetime(df["FECHA"], errors="coerce")
    df = pd.merge(df,tc,on=["FECHA"],how="left")
    df["MONTO($)"] = df["MONTO (S/)"]/df["TIPO_CAMBIO"]
    df = df.rename(columns={"MONTO (S/)": "MONTO_TP (S/)", "MONTO($)": "MONTO_TP($)"})
    return df


def builder_costo_laboral(dataframe,tc_df):
    #datos_costo_laboral
    costo_mo_cosecha = (
        dataframe[dataframe["PARTIDA PRESUPUESTARIA"] == "MO COSECHA"]
        .groupby(["FUNDO", "FECHA"], as_index=False)[["COSTO LABORAL", "N TRABAJADORES"]]
        .sum()
        .rename(columns={
            "COSTO LABORAL": "MO COSECHA_COSTO_CL",
            "N TRABAJADORES": "MO COSECHA_JR_CL",
        })
        )

        # Total diario (FECHA - FUNDO) cuando ACTIVIDAD es COSECHA
    costo_cosecha = (
            dataframe[dataframe["ACTIVIDAD"] == "COSECHA"]
            .groupby(["FUNDO", "FECHA"], as_index=False)[["COSTO LABORAL", "N TRABAJADORES"]]
            .sum()
            .rename(columns={
                "COSTO LABORAL": "COSECHA_COSTO_CL",
                "N TRABAJADORES": "COSECHA_JR_CL",
            })
        )

    costo_laboral_df = pd.merge(costo_mo_cosecha, costo_cosecha, on=["FUNDO", "FECHA"], how="outer")
    costo_laboral_df["FECHA"] = pd.to_datetime(costo_laboral_df["FECHA"], errors="coerce")
    dataframe = pd.merge(costo_laboral_df,tc_df,on=["FECHA"],how="left")
    dataframe["MO COSECHA_COSTO_CL"] = dataframe["MO COSECHA_COSTO_CL"].fillna(0)
    dataframe["COSECHA_COSTO_CL"] = dataframe["COSECHA_COSTO_CL"].fillna(0)
    dataframe["MO COSECHA_JR_CL"] = dataframe["MO COSECHA_JR_CL"].fillna(0)
    dataframe["COSECHA_JR_CL"] = dataframe["COSECHA_JR_CL"].fillna(0)

    dataframe["TIPO_CAMBIO"] = dataframe["TIPO_CAMBIO"].fillna(0)
    dataframe["MO COSECHA_COSTO$_CL"] = dataframe["MO COSECHA_COSTO_CL"]/dataframe["TIPO_CAMBIO"]
    dataframe["COSECHA_COSTO$_CL"] = dataframe["COSECHA_COSTO_CL"]/dataframe["TIPO_CAMBIO"]
    return dataframe


def builder_transporte_kias(transport_df,tc):
    transport_df.columns = (
                transport_df.columns.astype(str)
                .str.normalize('NFKD')
                .str.encode('ascii', errors='ignore')
                .str.decode('utf-8')
                .str.replace('\n', ' ', regex=False)
                .str.replace('.', '', regex=False)
                .str.strip()
                .str.upper()
        )
    transport_df = pd.merge(transport_df,tc,on=["FECHA"],how="left")
    transport_df["TARIFA $"] = transport_df["TARIFA"]/transport_df["TIPO_CAMBIO"]
    transport_df["FUNDO"] = transport_df["FUNDO"].str.strip()
    transport_df["FUNDO"] = transport_df["FUNDO"].replace({
        "QBERRIES":"QBERRIES II MAGICA",
        
    })
    #transport_df["CAMPAÑA"] = "CAMPAÑA 2026"
    #transport_df["VARIEDAD"] = "MAGICA"
    transport_df = transport_df.rename(columns={"TARIFA $":"MONTO_TK $"})
    #
    transport_df = transport_df[["SEMANA","FECHA","FUNDO","MONTO_TK $"]]
    return transport_df

def builder_transporte_camaras(dff,tc):
    dff = dff[dff["CAMPAÑA"]=="Campaña 2026"]
    dff = dff.rename(
        columns={
            "FECHA INICIO TRASLADO":"FECHA",
            "FUNDO PARTIDA":"FUNDO"
        }
    )
    dff["FUNDO"] = dff["FUNDO"].str.strip()
    dff["FUNDO"] = dff["FUNDO"].str.upper()
    dff["FECHA"] = pd.to_datetime(dff["FECHA"], errors="coerce")
    dff["FUNDO"] = dff["FUNDO"].replace({
        "GAP BERRIES":"GAP",
        "QBERRIES":"QBERRIES II MAGICA"
    })
    #print(dff["FUNDO"].unique())
    dff = dff.groupby(["FECHA","FUNDO"])[["COSTO PRORRATEADO"]].sum().reset_index()
    dff = dff.rename(
        columns={
            "COSTO PRORRATEADO":"MONTO_TI",
            
        }
    )
    dff = pd.merge(dff,tc,on=["FECHA"],how="left")
    dff["MONTO$_TI"] = dff["MONTO_TI"]/dff["TIPO_CAMBIO"]
    dff = dff.drop(columns = ["TIPO_CAMBIO"])
    return dff


def builder_costos_manual(df):
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
    df = df[[
        'FECHA','FUNDO', 'GRUPO COSECHA',
        'COSTO MO S/ GRUPO COSECHA', 'GRUPO  COSECHA DOLAR',
         'JORNALES', 'DISAL',
        'ALQUILER DE BANOS  VU DIARIO S/', 'COSTO DISAL DIARIO', 'COSTO DIS  $',
        'KG MATERIA PRIMA A PACKING', 'COMERCIAL DE HUERTO BUENO',
        'COMERCIAL DE HUERTO', 'KG COSECHADOS BRUTO', 'JARRAS', 'HA',
        '% COMERCIAL DE HUERTO', 'KG BRUTO/JR', 'JARRAS /JR', 'KG BRUTO /JARRA',
        'EPPS (POLOS, GORRAS, MASCARILLAS, ETC)', 'SUMINISTROS ACOPIO', 'OTROS',
        'ACTIVIDADES Y OTROS COSECHA (RRHH)', 'BIENESTAR SOCIAL (COSECHA)',
        'RECLUTAMIENTO Y SELECCION (COSECHA)', 'ADMINISTRACION DE PERSONAL (COSECHA)',
        'SERVICIOS CAMPO (COSECHA)'
    ]]

    col_numericos=[
        'GRUPO COSECHA',
        'COSTO MO S/ GRUPO COSECHA', 'GRUPO  COSECHA DOLAR',
         'JORNALES', 'DISAL',
        'ALQUILER DE BANOS  VU DIARIO S/', 'COSTO DISAL DIARIO', 'COSTO DIS  $',
        'KG MATERIA PRIMA A PACKING', 'COMERCIAL DE HUERTO BUENO',
        'COMERCIAL DE HUERTO', 'KG COSECHADOS BRUTO', 'JARRAS', 'HA',
        '% COMERCIAL DE HUERTO', 'KG BRUTO/JR', 'JARRAS /JR', 'KG BRUTO /JARRA',
        'EPPS (POLOS, GORRAS, MASCARILLAS, ETC)', 'SUMINISTROS ACOPIO', 'OTROS',
        'ACTIVIDADES Y OTROS COSECHA (RRHH)', 'BIENESTAR SOCIAL (COSECHA)',
        'RECLUTAMIENTO Y SELECCION (COSECHA)', 'ADMINISTRACION DE PERSONAL (COSECHA)',
        'SERVICIOS CAMPO (COSECHA)'
    ]
    for col_ in col_numericos:
        df[col_] = df[col_].fillna(0)
    #df = df.rename(columns={"CAMPANA":"CAMPAÑA"})
    df["FECHA"] = pd.to_datetime(df["FECHA"], errors="coerce")
    df = df.rename(columns={c: f"{c}_DN" for c in col_numericos if c in df.columns})
   
    return df

def _agg_to_fecha_fundo(df):
    group_keys = ["FECHA","FUNDO"] + (["MERCADO"] if "MERCADO" in df.columns else []) + (["PACKING"] if "PACKING" in df.columns else [])
    num_cols = [c for c in df.columns if c not in group_keys and pd.api.types.is_numeric_dtype(df[c])]
    df = df.groupby(group_keys)[num_cols].sum().reset_index()
    
    return df

def build_master_table():
    
    tc = datos_tipo_cambio_()
    #costo_laboral_df = builder_costo_laboral(builder_costo_laboral,tc)
    agri_df    = builder_agri_jr(datos_agritracer())
    cosecha_df = builder_cosecha(datos_cosecha_1())
   
    cosecha_df = _agg_to_fecha_fundo(cosecha_df)
    cosecha_df["TIPO_COS"] = cosecha_df["PACKING"].replace({
            "-":"-",
            "NACIONAL":"VENTA NACIONAL",
            
            "ALZA PERU PACKING": "ALZA PERU PACKING",
            "ALZA PACKING": "ALZA PERU PACKING",
            
        })
    #QBERRIES I, QBERRIES II MAGICA, QBERRIES II SEKOYA
    cosecha_df["TIPO_COS"] = cosecha_df["MERCADO"]
    fundos_qberries = ["QBERRIES I", "QBERRIES II MAGICA", "QBERRIES II SEKOYA"]
    mask_qberries = cosecha_df["FUNDO"].isin(fundos_qberries)
    cosecha_df.loc[mask_qberries, "MERCADO"] = np.where(
        cosecha_df.loc[mask_qberries, "MERCADO"] == "NACIONAL",
        "VENTA NACIONAL",
        "ALZA PERU PACKING",
    )
    cosecha_df.loc[mask_qberries, "TIPO_COS"] = cosecha_df.loc[mask_qberries, "MERCADO"]
    # La reasignacion de MERCADO para QBERRIES colapsa varios mercados
    # (CONVENCIONAL, ESPANA, etc.) en "ALZA PERU PACKING", lo que deja filas
    # duplicadas por (FECHA, FUNDO, MERCADO, PACKING). Se vuelven a agregar.
    cosecha_df = _agg_to_fecha_fundo(cosecha_df)
    cosecha_df["TIPO_COS"] = cosecha_df["MERCADO"]

    tp_df      = _agg_to_fecha_fundo(
        builder_transporte_personal(datos_transporte_personal(), tc)
        .drop(columns=["SEMANA","TIPO_CAMBIO"], errors="ignore")
    )
    ti_df      = (
        builder_transporte_camaras(datos_transporte_interno(), tc)
        .groupby(["FECHA","FUNDO"])[["MONTO_TI","MONTO$_TI"]].sum().reset_index()
    )
    tk_df      = (
        builder_transporte_kias(datos_transporte_kias(), tc)
        .groupby(["FECHA","FUNDO"])["MONTO_TK $"].sum().reset_index()
    )
    dn_df      = _agg_to_fecha_fundo(builder_costos_manual(datos_costos_manual()))
    cl_df      = _agg_to_fecha_fundo(                                                                      
            builder_costo_laboral(datos_costo_laboral(), tc)                                                   
              .drop(columns=["TIPO_CAMBIO"], errors="ignore")                                                    
    )  
    join_key = ["FECHA","FUNDO"]
    master = (
        agri_df
        .merge(cosecha_df, on=join_key, how="outer")
        .merge(tp_df,      on=join_key, how="left")
        .merge(ti_df,      on=join_key, how="left")
        .merge(tk_df,      on=join_key, how="left")
        .merge(dn_df,      on=join_key, how="left")
        .merge(cl_df,      on=join_key, how="left")  
    )
    master["TRANSPORTE_EXTERNO"] = 0
    # Columnas completamente vacías → 0 (no se eliminan)
    for col in master.columns:
        if master[col].isna().all():
            master[col] = 0
    num_cols = master.select_dtypes(include="number").columns
    master[num_cols] = master[num_cols].fillna(0)
    obj_cols = master.select_dtypes(include="object").columns
    master[obj_cols] = master[obj_cols].fillna("")
    # Convierte Int64 nullable a int64 estándar para compatibilidad con Power BI
    for col in master.columns:
        if str(master[col].dtype) == "Int64":
            master[col] = master[col].fillna(0).astype("int64")
    return master

#['CANYON MADEIRA' 'CANYON MAGICA' 'GAP BERRIES' 'LA COLINA' 'LAS BRISAS''LICAPA' 'NO ESPECIFICADO' 'QBERRIES II MAGICA' 'QBERRIES II SEKOYA''SAN JOSE' 'SAN JOSE II' 'SAN PEDRO']

#['SAN JOSE' 'SAN JOSE II' 'QBERRIES I' 'GAP' 'LAS BRISAS' 'SAN PEDRO''CANYON MADEIRA'  'LA COLINA' 'QBERRIES II MAGICA']
#df = pd.read_parquet("COSECHA CAMPO.parquet")


#df      = costo_proyectado_cosecha()
#df = data_cosecha()

#COSTO MANO DE OBR SOLO COSECHA
#st.dataframe(df)

#df = datos_costos_manual()
#print(df.columns)
#st.write(df.shape)
#st.dataframe(df)datos_transporte_personal()
tc = datos_tipo_cambio_()

df = builder_transporte_personal(datos_transporte_personal(), tc)

st.dataframe(df)


"""
cosecha_load_data()
st.success("COSECHA")
camaras_kias_load_data()
st.success("TRANSPORTE")
load_costo_laboral_gh()
st.success("costo laboral cargado")

df = build_master_table()
hoy_peru = pd.Timestamp.now(tz="America/Lima").normalize().tz_localize(None)
df = df[df["FECHA"].dt.normalize() != hoy_peru]
#print(df.columns)
st.write(df.shape)
st.dataframe(df)

access_token = get_access_token()
resultado = subir_archivo_con_reintento(
        access_token=access_token,
        dataframe=df,
        nombre_archivo="COSECHA_MAESTRO.parquet",
        drive_id="b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        folder_id="01KM43WT4FS6JNXKKHRNCJZMAXLX56IOEQ",
        type_file="parquet"
    )
if resultado:
    print(f"✅ Proceso completado exitosamente")

else:
    print(f"❌ Error al subir el archivo")
"""                                                                                                                                     
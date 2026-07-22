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



from functions.costos import *
from functions.transporte import *
from functions.transporte import desagregar_transporte

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
            "b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR",
            "01XOBWFSDC2SPT2RFM3BGY6TJUHKMNQGOI"
        )
        url_ = get_download_url_by_name(data, "TRANSPORTE_KIAS.parquet")
        df = pd.read_parquet(url_)

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
    
    
    
    agritracer_df = agritracer_df.groupby(["FUNDO","YEAR","FECHA","PERIODO SEMANA","SEMANA","ACTIVIDAD","PARTIDA PRESUPUESTARIA","MACRO PARTIDA","TRABAJADOR"]).agg({"COPIA": "count","HORAS": "sum"}).reset_index()#,"HORAS": "sum"
    agritracer_df = agritracer_df.groupby(["FUNDO","YEAR","FECHA","SEMANA","PERIODO SEMANA","ACTIVIDAD","PARTIDA PRESUPUESTARIA","MACRO PARTIDA"]).agg({"COPIA": "count","HORAS": "sum"}).reset_index()#,"HORAS": "sum"
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

    
    HORAS_POR_JORNAL = 9.6
    
    mo_cosecha_mask = agritracer_df["PARTIDA PRESUPUESTARIA"] == "MO COSECHA"
    cosecha_mask = agritracer_df["ACTIVIDAD"] == "COSECHA"
    
    mo_cosecha_df = (
        agritracer_df[mo_cosecha_mask]
        .groupby(index_cols)[["JORNALES_AGRITRACER", "HORAS"]].sum().reset_index()
        .rename(columns={"JORNALES_AGRITRACER": "MO COSECHA", "HORAS": "MO COSECHA_JRH"})
    )
    mo_cosecha_df["MO COSECHA_JRH"] = mo_cosecha_df["MO COSECHA_JRH"] / HORAS_POR_JORNAL
    
    cosecha_df = (
        agritracer_df[cosecha_mask]
        .groupby(index_cols)[["JORNALES_AGRITRACER", "HORAS"]].sum().reset_index()
        .rename(columns={"JORNALES_AGRITRACER": "COSECHA", "HORAS": "COSECHA_JRH"})
    )
    cosecha_df["COSECHA_JRH"] = cosecha_df["COSECHA_JRH"] / HORAS_POR_JORNAL
    
    pivot_df = pivot_df.merge(mo_cosecha_df, on=index_cols, how="left").merge(cosecha_df, on=index_cols, how="left")
    
    fill_cols = ["MO COSECHA", "COSECHA", "MO COSECHA_JRH", "COSECHA_JRH"]
    pivot_df[fill_cols] = pivot_df[fill_cols].fillna(0)
    pivot_df = pivot_df.rename(columns = {"MO COSECHA":"MO COSECHA_JR","COSECHA":"COSECHA_JR"})
    return pivot_df

# Variantes de PACKING que llegan desde cosecha y que representan al mismo destino.
# Sin normalizar, el groupby por PACKING abre una fila por variante y el master
# termina con filas duplicadas para la misma FECHA/FUNDO.
PACKING_NORMALIZADO = {
    "QPACK": "ALZA PERU PACKING",
    "Q PACK": "ALZA PERU PACKING",
    "Q PACK PERU": "ALZA PERU PACKING",
    "QPACK PERU": "ALZA PERU PACKING",
    "ALZA PACKING": "ALZA PERU PACKING",
    "ALZA PERU PACKING": "ALZA PERU PACKING",
    "NACIONAL": "VENTA NACIONAL",
    "VENTA NACIONAL": "VENTA NACIONAL",
}

def normalizar_packing(serie):
    limpio = (
        serie.astype(str)
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
        .str.strip()
        .str.upper()
        .str.replace(r"\s+", " ", regex=True)
    )
    return limpio.map(PACKING_NORMALIZADO).fillna(limpio)

def builder_cosecha(df):
    df["FECHA"] = pd.to_datetime(df["FECHA"], errors="coerce")
    df = df[df["FECHA"]>="2026-06-01"]
    df["FUNDO_"] = df["FUNDO_"].replace({
        'CANYON MANILA':"CANYON MADEIRA"
    })
    df = df.rename(columns = {"FUNDO":"FUNDO_ALTERNO"})
    df = df.rename(columns = {"FUNDO_":"FUNDO"})
    df = df.drop(columns = ["CAMPAÑA"])
    df["PACKING"] = normalizar_packing(df["PACKING"])

    df = df.groupby(["FECHA","FUNDO","FUNDO_ALTERNO","PACKING"])[["HA","JORNAL","JABAS","JARRAS","KILOS BRUTOS","KILOS /HA","DESCARTE"]].sum().reset_index()
    

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


# El prorrateo de QBERRIES/CANYON entre sus fundos hijos vive ahora en
# functions/transporte.py (_prorratear_etapas / desagregar_transporte) y viene
# embebido por etapa en los parquet TRANSPORTE_CAMARAS/KIAS. Los builders solo
# desagregan esas columnas ETAPA a filas por fundo hijo.

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
    transport_df["FECHA"] = pd.to_datetime(transport_df["FECHA"], errors="coerce")
    transport_df["FUNDO"] = transport_df["FUNDO"].str.strip()
    transport_df["FUNDO"] = transport_df["FUNDO"].replace("TARA","LAS BRISAS")
    # El reparto QBERRIES/CANYON viene embebido por etapa en TRANSPORTE_KIAS.parquet.
    # Desagregamos esas columnas ETAPA a una fila por (FECHA, fundo hijo) sin
    # duplicar el resto de columnas del viaje. KILOS_COS = kilos cosechados de ese
    # fundo/etapa en esa fecha.
    df = desagregar_transporte(transport_df, "FECHA", "FUNDO", "TARIFA", "MONTO_TK")
    df = df.groupby(["FECHA","FUNDO"], as_index=False).agg(
        MONTO_TK=("MONTO_TK","sum"),
        KILOS_COS=("KILOS_COS","sum"),
    )
    df = pd.merge(df,tc,on=["FECHA"],how="left")
    df["MONTO_TK $"] = df["MONTO_TK"]/df["TIPO_CAMBIO"]
    df = df.drop(columns=["TIPO_CAMBIO"])
    return df

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
        "TARA":"LAS BRISAS"
    })
    #print(dff["FUNDO"].unique())
    # El reparto QBERRIES/CANYON viene embebido por etapa en TRANSPORTE_CAMARAS.parquet.
    # Desagregamos esas columnas ETAPA a una fila por (FECHA, fundo hijo) sin duplicar
    # el resto de columnas del viaje. KILOS_COS = kilos cosechados de ese fundo/etapa.
    dff = desagregar_transporte(dff, "FECHA", "FUNDO", "COSTO PRORRATEADO", "MONTO_TI")
    dff = dff.groupby(["FECHA","FUNDO"], as_index=False).agg(
        MONTO_TI=("MONTO_TI","sum"),
        KILOS_COS=("KILOS_COS","sum"),
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
    df["FUNDO"] = df["FUNDO"].replace("TARA","LAS BRISAS")
    for col_ in col_numericos:
        df[col_] = pd.to_numeric(df[col_], errors="coerce").fillna(0)
    #df = df.rename(columns={"CAMPANA":"CAMPAÑA"})
    df["FECHA"] = pd.to_datetime(df["FECHA"], errors="coerce")
    df = df.rename(columns={c: f"{c}_DN" for c in col_numericos if c in df.columns})
    
    return df

def _agg_to_fecha_fundo(df):
    group_keys = ["FECHA","FUNDO"] + (["PACKING"] if "PACKING" in df.columns else [])
    num_cols = [c for c in df.columns if c not in group_keys and pd.api.types.is_numeric_dtype(df[c])]
    df = df.groupby(group_keys)[num_cols].sum().reset_index()
    
    return df

def build_master_table():
    
    tc = datos_tipo_cambio_()
    #costo_laboral_df = builder_costo_laboral(builder_costo_laboral,tc)
    agri_df    = builder_agri_jr(datos_agritracer())
    cosecha_df = builder_cosecha(datos_cosecha_1())
   
    cosecha_df = _agg_to_fecha_fundo(cosecha_df)
    
    # TIPO_COS se deriva de PACKING despues de la agregacion, porque
    # _agg_to_fecha_fundo descarta las columnas de texto que no son clave de grupo.
    # PACKING ya viene normalizado desde builder_cosecha.
    cosecha_df["TIPO_COS"] = cosecha_df["PACKING"]
    #cosecha_df["TIPO_COS"] = cosecha_df["MERCADO"]

    tp_df      = _agg_to_fecha_fundo(
        builder_transporte_personal(datos_transporte_personal(), tc)
        .drop(columns=["SEMANA","TIPO_CAMBIO"], errors="ignore")
    )
    print("1")
    ti_df      = (
        builder_transporte_camaras(datos_transporte_interno(), tc)
        .groupby(["FECHA","FUNDO"])[["MONTO_TI","MONTO$_TI"]].sum().reset_index()
    )
    print("2")
    tk_df      = (
        builder_transporte_kias(datos_transporte_kias(), tc)
        .groupby(["FECHA","FUNDO"])["MONTO_TK $"].sum().reset_index()
    )
    print("3")
    dn_df      = _agg_to_fecha_fundo(builder_costos_manual(datos_costos_manual()))
    print("4")
    cl_df      = _agg_to_fecha_fundo(                                                                      
            builder_costo_laboral(datos_costo_laboral(), tc)                                                   
              .drop(columns=["TIPO_CAMBIO"], errors="ignore")                                                    
    )  
    print("5")
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



#camaras_kias_load_data()
#st.success("TRANSPORTE")

#cosecha_load_data()
#st.success("COSECHA")

#load_costo_laboral_gh()
#st.success("costo laboral cargado")

df = costo_proyectado_cosecha()
st.dataframe(df)
"""
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

#dn_df      = _agg_to_fecha_fundo(builder_costos_manual(datos_costos_manual()))
#st.dataframe(dn_df)
"""
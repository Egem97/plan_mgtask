import streamlit as st
import pandas as pd
from utils.utils import *
from utils.helpers import get_download_url_by_name
from utils.get_api import listar_archivos_en_carpeta_compartida,subir_archivo_con_reintento
from utils.utils import read_excel_fast
from utils.get_token import get_access_token

def camaras_data():
    data = listar_archivos_en_carpeta_compartida(
        get_access_token(),
        "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        "01KM43WT4M3PZTZVTV3VC3ORHLY6J4WI2X"
    )
    url_excel_1 = get_download_url_by_name(data, "APG ALZA - CONTROL T. CÁMARAS.xlsx")
    url_excel_2 = get_download_url_by_name(data, "APG TK - CONTROL DIARIO KIAS.xlsx")
    url_excel_3 = get_download_url_by_name(data, "CONTROL T. CÁMARAS ABG ALZA.xlsx")
    
    return (
        read_excel_fast(url_excel_1, sheet_name="BD", skiprows=2),
        read_excel_fast(url_excel_2, sheet_name="BD"),
        read_excel_fast(url_excel_3, sheet_name="BD", skiprows=2)
    )

def transform_camaras_kias():
    control_camaras,apg_kias,control_camaras25 = camaras_data()
    control_camaras["TIPO"] = "APG"
    control_camaras25["TIPO"] = "COMPARTIDO"
    control_camaras25 = control_camaras25.rename(columns = {'TIPO UN.':'TIPO UNID.'})
    camaras_df = pd.concat([control_camaras,control_camaras25])
    camaras_df.columns = (
            camaras_df.columns.astype(str)
            .str.normalize('NFKD')
            .str.encode('ascii', errors='ignore')
            .str.decode('utf-8')
            .str.replace('\n', ' ', regex=False)
            .str.replace('.', '', regex=False)
            .str.strip()
            .str.upper()
    )
    cols = ['SEM','FECHA INICIO TRASLADO', 'RUTA', 'CONDICION RUTA', 'GREM FUNDO',
       'GREM TRANSPORT', 'FACTURA', 'PLACA', 'TIPO UNID',
        'FUNDO PARTIDA', 'PACKING', 'N VIAJE VALIDADO',
       'H INICIO', 'HORA FIN', 'N PALLETS', 'PESO TRANSPORTADO',
        'NOMBRE TRANSPORTISTA',
       'CONDUCTOR', 'DIRECCION FUNDO', 'NOMBRE REMITENTE',
       'NOMBRE DESTINATARIO', 'CAPACIDAD PALLETS',
       'CAPAC (KG)', 'TIEMPO', 'COD RUTA', 'COSTO', 'COSTO PRORRATEADO',
       'PESO TOTAL DEL VIAJE', 'COSTO / KG', '% OCUP','TIPO',
    ]
    camaras_df = camaras_df[cols]
    camaras_df = camaras_df[(camaras_df["FECHA INICIO TRASLADO"].notna())]
    cols_str = [
        'RUTA', 'CONDICION RUTA', 'GREM FUNDO','GREM TRANSPORT', 'FACTURA', 'PLACA', 'TIPO UNID',
        'FUNDO PARTIDA', 'PACKING', 'N VIAJE VALIDADO','NOMBRE TRANSPORTISTA',
        'CONDUCTOR', 'DIRECCION FUNDO', 'NOMBRE REMITENTE',
        'NOMBRE DESTINATARIO','COD RUTA',
    ]
    cols_num = [
        'N PALLETS', 'PESO TRANSPORTADO','CAPACIDAD PALLETS',
        'CAPAC (KG)','COSTO', 'COSTO PRORRATEADO',
        'PESO TOTAL DEL VIAJE', 'COSTO / KG', '% OCUP','SEM',
    ]
    cols_time = [
        'H INICIO', 'HORA FIN','TIEMPO'
    ]

    for cstr in cols_str:
        camaras_df[cstr] = camaras_df[cstr].fillna("-")
        camaras_df[cstr] = camaras_df[cstr].astype(str)
        camaras_df[cstr] = camaras_df[cstr].str.strip()
        camaras_df[cstr] = camaras_df[cstr].str.upper()
        
    for cnum in cols_num:
        camaras_df[cnum] = camaras_df[cnum].fillna(0)
        camaras_df[cnum] = camaras_df[cnum].astype(str)
        camaras_df[cnum] = camaras_df[cnum].astype(float)
        
    for coltime in cols_time:
        #camaras_df[coltime] = camaras_df[coltime].fillna('00:00:00') 
        camaras_df[coltime] = pd.to_datetime(camaras_df[coltime], format='%H:%M:%S', errors='coerce').dt.time

    camaras_df["FECHA INICIO TRASLADO"] = pd.to_datetime(camaras_df["FECHA INICIO TRASLADO"]).dt.date
    camaras_df["RUTA"] = camaras_df["RUTA"].str.split("-").str[0].str.strip()
    camaras_df["CAMPAÑA"] = 2025
    camaras_df = camaras_df.rename(columns = {"SEM":"SEMANA"})
    """
    """
    apg_kias.columns = (
            apg_kias.columns.astype(str)
            .str.normalize('NFKD')
            .str.encode('ascii', errors='ignore')
            .str.decode('utf-8')
            .str.replace('\n', ' ', regex=False)
            .str.replace('.', '', regex=False)
            .str.strip()
            .str.upper()
    )

    cols_kias =['SEMANA','FECHA', 'FUNDO', 'ACTIVIDAD', 'SERVICIO',
        'VALIDACION DETALLE DE VIAJE', 'RS FUNDO', 'PLACA',
        'PLACA REG', 'GRR', 'FACTURA', 'CONDUCTOR',
        'HORA ENTRADA', 'HORA SALIDA', 
        'CAPAC TOTAL JARRAS', 'ZONA', 'RAZON SOCIAL-PROVEEDOR',
            'PROVEEDOR-ZONA', 'TARIFA', 'TARIFA TOTAL',
        'COD TARIF PRO', 'MODULOS QBERRIES']
    apg_kias = apg_kias[cols_kias]
    apg_kias = apg_kias[(apg_kias["FECHA"].notna())]
    cols_str_kias = ['FUNDO', 'ACTIVIDAD', 'SERVICIO',
        'VALIDACION DETALLE DE VIAJE', 'RS FUNDO', 'PLACA',
        'PLACA REG', 'FACTURA', 'CONDUCTOR',
            'ZONA', 'RAZON SOCIAL-PROVEEDOR',
            'PROVEEDOR-ZONA', 
        'COD TARIF PRO', 'MODULOS QBERRIES'
    ]
    cols_num_kias = [ 'GRR',  'CAPAC TOTAL JARRAS', 'TARIFA', 'TARIFA TOTAL','SEMANA']
    cols_time_kias = [
            'HORA ENTRADA', 'HORA SALIDA',
    ]
    for cstrk in cols_str_kias:
            apg_kias[cstrk] = apg_kias[cstrk].fillna("-")
            apg_kias[cstrk] = apg_kias[cstrk].astype(str)
            apg_kias[cstrk] = apg_kias[cstrk].str.strip()
            apg_kias[cstrk] = apg_kias[cstrk].str.upper()

    for cnumk in cols_num_kias:
            apg_kias[cnumk] = apg_kias[cnumk].fillna(0)
            apg_kias[cnumk] = apg_kias[cnumk].astype(str)
            apg_kias[cnumk] = apg_kias[cnumk].astype(float)

    for coltimek in cols_time_kias:
        apg_kias[coltimek] = pd.to_datetime(apg_kias[coltimek]).dt.time

    apg_kias["FECHA"] = pd.to_datetime(apg_kias["FECHA"]).dt.date
    apg_kias["MODULOS QBERRIES"] = (
    apg_kias["MODULOS QBERRIES"]
        .str.normalize('NFKD')
        .str.encode('ascii', errors='ignore')
        .str.decode('utf-8')
    )
    apg_kias["CAMPAÑA"] = 2025
    #.str.normalize('NFKD')
    return camaras_df,apg_kias

def camaras_kias_load_data():
    print(f"📤 Subiendo archivos 'CAMARA Y KIAS' a OneDrive...")
    camaras_df, kias_df =transform_camaras_kias()
    resultado_1 = subir_archivo_con_reintento(
        access_token=get_access_token(),
        dataframe=camaras_df,
        nombre_archivo="TRANSPORTE_CAMARAS.parquet",
        drive_id="b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR",
        folder_id="01XOBWFSDC2SPT2RFM3BGY6TJUHKMNQGOI",
        type_file="parquet"
    )
    if resultado_1:
        print(f"✅ Proceso 1 completado exitosamente")
    resultado_2 = subir_archivo_con_reintento(
        access_token=get_access_token(),
        dataframe=kias_df,
        nombre_archivo="TRANSPORTE_KIAS.parquet",
        drive_id="b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR",
        folder_id="01XOBWFSDC2SPT2RFM3BGY6TJUHKMNQGOI",
        type_file="parquet"
    )
    if resultado_2:
        print(f"✅ Proceso 2 completado exitosamente")
    else:
        print(f"❌ Error al subir el archivo")
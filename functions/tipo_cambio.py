import pandas as pd
from utils.utils import *
from utils.helpers import get_download_url_by_name
from utils.get_api import listar_archivos_en_carpeta_compartida,subir_archivo_con_reintento,get_tc_sunat_diario
from pandas.core.frame import DataFrame
from utils.get_token import get_access_token



def tipo_cambio_extract(access_token):
    print(f"📁 Obteniendo datos de tipo de cambio: ")
    data = listar_archivos_en_carpeta_compartida(
            access_token,
            "b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR",
            "01XOBWFSDI34HN5SD7HBHZRUBPX3457IPQ"
    )
    url_excel = get_download_url_by_name(data, "TIPO DE CAMBIO.xlsx")
    if not url_excel:
        print(f"❌ No se encontró el archivo de Tipo de Cambio: TIPO DE CAMBIO.xlsx")
        return False
    return pd.read_excel(url_excel)

def tipo_cambio_transform(access_token):
    now = datetime.now().strftime('%Y-%m-%d')
    df = tipo_cambio_extract(access_token)
    df["FECHA"] = pd.to_datetime(df["FECHA"])
    ultima_fecha = df["FECHA"].max()
    fecha_str = ultima_fecha.strftime('%Y-%m-%d')
    if now >fecha_str:
        print(f"🔍 Buscando tipo de cambio para la fecha: {now}")
        json_tc = get_tc_sunat_diario(date= now)
        df_tc = pd.DataFrame([json_tc])
        df_tc.columns = ["PrecioCompra","PrecioVenta","Moneda","FECHA"]
        df_tc["FECHA"] = pd.to_datetime(df_tc["FECHA"])
        df = pd.concat([df,df_tc])
        return df
    else:
        print(f"🔍 Tipo de cambio para la fecha: {now} ya existe")
        return df

def tipo_cambio_load_data():
    access_token = get_access_token()
    df = tipo_cambio_transform(access_token)
    print(f"📤 Subiendo archivo TIPO DE CAMBIO.xlsx a OneDrive...")
    resultado = subir_archivo_con_reintento(
        access_token=access_token,
        dataframe=df,
        nombre_archivo="TIPO DE CAMBIO.xlsx",
        drive_id="b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR",
        folder_id="01XOBWFSDI34HN5SD7HBHZRUBPX3457IPQ"
    )

    if resultado:
        print(f"✅ Proceso completado exitosamente")
        print(f"📁 Archivo subido: TIPO DE CAMBIO.xlsx")
        return True
    else:
        print(f"❌ Error al subir el archivo")
        return False
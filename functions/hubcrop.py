import pandas as pd
import mysql.connector
from utils.config import load_config
from functions.agricola import format_lote
from utils.get_token import get_access_token
from utils.get_api import listar_archivos_en_carpeta_compartida,subir_archivo_con_reintento,get_tc_sunat_diario
from utils.helpers import get_download_url_by_name

def get_hubcrop_connection():
    """Establishes a connection to the HubCrop database."""
    config = load_config()
    if not config:
        st.error("Error: No configuration loaded from config.yaml")
        return None
    
    creds = config.get('hubcrop')
    if not creds:
        st.error("Error: 'hubcrop' section not found in config.yaml")
        return None

    try:
        conn = mysql.connector.connect(
            host=creds.get('host'),
            port=creds.get('port', 3306),
            database=creds.get('database'),
            user=creds.get('user'),
            password=creds.get('password')
        )
        return conn
    except mysql.connector.Error as err:
        st.error(f"Error connecting to HubCrop: {err}")
        return None

def run_hubcrop_query(query):
    """Executes a SQL query against HubCrop and returns a DataFrame."""
    conn = get_hubcrop_connection()
    if not conn:
        return pd.DataFrame() # Return empty DataFrame on failure
    
    try:
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"Error executing query: {e}")
        return pd.DataFrame()
    finally:
        if conn.is_connected():
            conn.close()

def get_hubcrop_cosecha_default():
    query = """
    select   
    HUERTO,
    CUARTEL,
    RUT_TRABAJADOR,
    NOMBRE_TRABAJADOR,
    FECHA,
    SUM(PESO * BANDEJAS) AS KILOS
    from hubcrop_alsa_cosecha
    group by HUERTO,CUARTEL,RUT_TRABAJADOR,NOMBRE_TRABAJADOR,FECHA
    having FECHA >='2026-01-01'and FECHA <='2026-12-31';


    """
    return run_hubcrop_query(query)



def clean_hubcrop(df):
    #hubcrop25_df = pd.read_parquet("HUBCROP_2025.parquet")
    #hubcrop25_df = hubcrop25_df.drop(columns=["VARIEDAD"])
    df["FECHA"] = pd.to_datetime(df["FECHA"]).dt.date
    df["HUERTO"] = df["HUERTO"].astype("string")
    df["HUERTO"] = df["HUERTO"].str.strip()
    df["HUERTO"] = df["HUERTO"].str.upper()
    df["HUERTO"] = df["HUERTO"].replace({
        "GAP": "GAP BERRIES",

    })

    df["CUARTEL"] = df["CUARTEL"].astype("string")
    df["CUARTEL"] = df["CUARTEL"].str.strip()
    df["CUARTEL"] = df["CUARTEL"].str.replace("-I", "I")
    df[['LOTE', 'TURNO', 'MODULO']] = df['CUARTEL'].str.split('-',n=2,expand=True)
    df["RUT_TRABAJADOR"] = df["RUT_TRABAJADOR"].astype("string")
    df["RUT_TRABAJADOR"] = df["RUT_TRABAJADOR"].str.strip()
    df["RUT_TRABAJADOR"] = df["RUT_TRABAJADOR"].str.replace("-E", "")
    df["RUT_TRABAJADOR"] = df["RUT_TRABAJADOR"].str.replace("-0", "")
    df["NOMBRE_TRABAJADOR"] = df["NOMBRE_TRABAJADOR"].astype("string")
    df["NOMBRE_TRABAJADOR"] = df["NOMBRE_TRABAJADOR"].str.strip()
    df["KILOS"] = df["KILOS"].astype(float)

    df[['LOTE', 'TURNO', 'MODULO']] = df['CUARTEL'].str.split('-',n=2,expand=True)
    df["LOTE"] = df["LOTE"].astype("string")
    df["LOTE"] = df["LOTE"].str.strip()
    df["LOTE"] = df["LOTE"].str.replace("I", "-1")
    df["LOTE"] = df["LOTE"].str.replace("L", "")
    df["LOTE"] = df["LOTE"].apply(format_lote)
    df["TURNO"] = df["TURNO"].astype("string")
    df["TURNO"] = df["TURNO"].str.strip()
    df["TURNO"] = df["TURNO"].str.replace("T", "")
    df["MODULO"] = df["MODULO"].astype("string")
    df["MODULO"] = df["MODULO"].str.strip()
    return df

def pipeline_hubcrop():
    drive_id = "b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR"
    folder_id = "01XOBWFSAWRK2MNONLCZAIMP753DZ3SXTC"
    access_token = get_access_token()
    def historico_hubcrop(access_token):
        data = listar_archivos_en_carpeta_compartida(
            access_token,
            drive_id,
            folder_id
        )
        url_parquet = get_download_url_by_name(data, "HUBCROP_2025_.parquet")
        
        return pd.read_parquet(url_parquet, engine="pyarrow") 

    hdf = historico_hubcrop(access_token)
    df = get_hubcrop_cosecha_default()
    df = clean_hubcrop(df)
    df = pd.concat([hdf, df], ignore_index=True)
    access_token = get_access_token()
    print(f"📤 Subiendo archivo 'HUBCROP' a OneDrive...")
    
    resultado = subir_archivo_con_reintento(
        access_token=access_token,
        dataframe=df,
        nombre_archivo="HUBCROP_GENERAL.parquet",
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
    
    
import pandas as pd
import os
from utils.get_kiss import fetch_all_kissflow
from utils.get_token import get_access_token
from utils.get_api import subir_archivo_con_reintento

def datakiss_formularios_camaras():
    df_kissflow = fetch_all_kissflow("BD_FORMULARIO_CAMARAS")
    df_kissflow = df_kissflow.rename(columns={
        'Untitled_field':'FUNDO',
        'N_DE_PLACA':'PLACA',
        'Untitled_field_1':'SE_APRUEBA',
        'HORA_DE_LLEGADA':'FECHA_DE_LLEGADA'
    })
    print(df_kissflow.columns)
    df_kissflow["FECHA_DE_LLEGADA"] = pd.to_datetime(
        df_kissflow["FECHA_DE_LLEGADA"].astype(str).str.split(" ").str[0],
        errors="coerce"
    ).dt.tz_localize(None)
    
    df_kissflow["FECHA_Y_HORA_DE_REGISTRO"] = pd.to_datetime(
        df_kissflow["FECHA_Y_HORA_DE_REGISTRO"], errors="coerce", utc=True
    )
    df_kissflow["FECHA_Y_HORA_DE_REGISTRO"] = df_kissflow[
        "FECHA_Y_HORA_DE_REGISTRO"
    ].dt.tz_convert("America/Lima").dt.tz_localize(None)

    df_kissflow["NOMBRE_DEL_CONDUCTOR"] = df_kissflow["NOMBRE_DEL_CONDUCTOR"].str.upper()
    df_kissflow["RESPONSABLE_DE_CONSOLIDADO"] = df_kissflow["RESPONSABLE_DE_CONSOLIDADO"].str.upper()
    return df_kissflow

def upload_datakiss_bd_formulario_camaras():
    print(f"📤 Subiendo archivos 'BD_FORMULARIO_CAMARAS' a OneDrive...")
    df = datakiss_formularios_camaras()
    token = get_access_token()
    drive_id = "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s"
    folder_id = "01KM43WTYRYBQEKV2BE5CZJCDE46QO7POH"

    ok_parquet = subir_archivo_con_reintento(
        access_token=token,
        dataframe=df,
        nombre_archivo="BD_FORMULARIO_CAMARAS.parquet",
        drive_id=drive_id,
        folder_id=folder_id,
        type_file="parquet"
    )

    ok_excel = subir_archivo_con_reintento(
        access_token=token,
        dataframe=df,
        nombre_archivo="BD_FORMULARIO_CAMARAS.xlsx",
        drive_id=drive_id,
        folder_id=folder_id,
        type_file="excel"
    )

    if ok_parquet and ok_excel:
        print(f"✅ Parquet y Excel subidos exitosamente")
        return True
    else:
        print(f"❌ Error: parquet={'✅' if ok_parquet else '❌'}  excel={'✅' if ok_excel else '❌'}")
        return False
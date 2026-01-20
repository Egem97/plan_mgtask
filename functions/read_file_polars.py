
import pandas as pd
import polars as pl
import requests
import numpy as np
from typing import Optional 





def read_excel_fast(source: str, sheet_name: str | None = None, skiprows: int | None = None):
    try:
        kwargs = {}
        if sheet_name is not None:
            kwargs["sheet_name"] = sheet_name
        if skiprows is not None:
            kwargs["skip_rows"] = skiprows

        df_pl = pl.read_excel(source, **kwargs)
        return df_pl.to_pandas()
    except Exception as e:
        try:
            return pd.read_excel(source, sheet_name=sheet_name, skiprows=skiprows)
        except Exception as e2:
            print(f"Error leyendo Excel (polars/pandas): {e} | fallback error: {e2}")
            return None

def get_download_url_by_name(json_data, name):
    for item in json_data:
        if item.get('name') == name:
            return item.get('@microsoft.graph.downloadUrl')

def listar_archivos_en_carpeta_compartida(access_token: str  ,drive_id: str, item_id: str):

    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/children"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json().get("value", [])
    else:
        print("Error al obtener archivos:", response.status_code)
        print(response.json())
        return []

def get_access_token() -> Optional[str]:
    if not config:
        print("Error: No se pudo cargar la configuración")
        return None
    
    AUTHORITY = f"https://login.microsoftonline.com/{config['microsoft_graph']['tenant_id']}/oauth2/v2.0/token"# cambiar por credenciales
    try:
        response = requests.post(AUTHORITY, data={
            "grant_type": "client_credentials",
            "client_id": config['microsoft_graph']['client_id'],# cambiar por credenciales
            "client_secret": config['microsoft_graph']['client_secret'],# cambiar por credenciales
            "scope": "https://graph.microsoft.com/.default"
        })
        
        if response.status_code == 200:
            token_response = response.json()
            access_token = token_response.get("access_token")
            
            if access_token:
                print("Token de acceso obtenido exitosamente")
                return access_token
            else:
                print("Error: No se pudo obtener el token de acceso")
                return None
        else:
            print(f"Error HTTP {response.status_code}: {response.text}")
            return None
            
    except Exception as e:
        print(f"Error al obtener el token: {e}")
        return None



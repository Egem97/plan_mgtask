import pandas as pd
import polars as pl
import numpy as np
from datetime import datetime
import os
import zipfile
import tempfile
import unicodedata
import re
import requests

def _sanitize_xlsx_remove_styles(src_path: str) -> str:
    """Create a temporary copy of the XLSX without styles/calcChain to bypass invalid XML.
    Returns path to the sanitized temporary XLSX.
    """
    fd, dst_path = tempfile.mkstemp(suffix=".xlsx")
    os.close(fd)
    with zipfile.ZipFile(src_path, "r") as zin, zipfile.ZipFile(
        dst_path, "w", compression=zipfile.ZIP_DEFLATED
    ) as zout:
        for item in zin.infolist():
            name = item.filename
            # Drop styles and calcChain which are common sources of corruption
            if name in ("xl/styles.xml", "xl/calcChain.xml"):
                continue
            data = zin.read(name)
            # Preserve ZipInfo metadata when writing
            zout.writestr(item, data)
    return dst_path


def read_excel_resilient(path: str, **kwargs):
    """Read Excel robustly. If invalid XML/styles error, try a sanitized copy.
    Returns a DataFrame or None on failure (and reports via Streamlit UI).
    """
    temp_file = None
    try:
        # Si es una URL, descargar primero
        if path.startswith("http://") or path.startswith("https://"):
            print(f"Descargando archivo desde URL...")
            response = requests.get(path)
            response.raise_for_status()
            
            # Crear archivo temporal
            fd, temp_path = tempfile.mkstemp(suffix=".xlsx")
            os.close(fd)
            with open(temp_path, "wb") as f:
                f.write(response.content)
            
            path = temp_path
            temp_file = temp_path

        return pd.read_excel(path, **kwargs)
    except Exception as e:
        msg = str(e).lower()
        if "stylesheet" in msg or "invalid xml" in msg or "could not read stylesheet" in msg:
            print(
                "El libro tiene estilos/ XML inválido. Intentando leer una copia saneada temporal."
            )
            try:
                sanitized = _sanitize_xlsx_remove_styles(path)
                df = pd.read_excel(sanitized, **kwargs)
                print("Lectura exitosa usando copia saneada (sin estilos).")
                # Limpiar archivo saneado
                try:
                    os.remove(sanitized)
                except:
                    pass
                return df
            except Exception as e2:
                print(
                    "No se pudo leer el Excel incluso tras saneo. Abra el archivo en Excel y use 'Abrir y reparar' o 'Guardar como' .xlsx para regenerarlo.\n"
                    + f"Detalle: {e2}"
                )
                return None
        else:
            print(f"Error leyendo el Excel: {e}")
            return None
    finally:
        # Limpiar archivo temporal de descarga si existe
        if temp_file and os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except:
                pass


def read_excel_fast(source: str, sheet_name: str | None = None, skiprows: int | None = None):
    """
    Lee Excel de forma eficiente usando Polars si está disponible y hace fallback a pandas.

    Retorna un pandas.DataFrame para mantener compatibilidad con el resto del código.
    """
    try:
        kwargs = {}
        if sheet_name is not None:
            kwargs["sheet_name"] = sheet_name
        if skiprows is not None:
            # Polars usa 'skip_rows' para saltar filas iniciales
            kwargs["skip_rows"] = skiprows

        df_pl = pl.read_excel(source, **kwargs)
        return df_pl.to_pandas()
    except Exception as e:
        # Fallback a pandas si falla Polars
        try:
            return pd.read_excel(source, sheet_name=sheet_name, skiprows=skiprows)
        except Exception as e2:
            print(f"Error leyendo Excel (polars/pandas): {e} | fallback error: {e2}")
            return None


def clean_turno(v):
    """Normaliza valores de TURNO: floats/ints a int, quita letras, y si hay comas usa el primer número.
    Devuelve 0 si no hay número válido.
    """
    try:
        if pd.isna(v):
            return 0
    except Exception:
        pass
    if isinstance(v, (int, np.integer)):
        return int(v)
    if isinstance(v, (float, np.floating)):
        return int(v)
    s = str(v).strip()
    if not s:
        return 0
    first = s.split(",")[0]
    m = re.search(r"\d+", first)
    if m:
        return int(m.group(0))
    return 0


def sanitize_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    """Asegura dtypes compatibles con Parquet/Power BI y normaliza fechas.

    - Convierte 'FECHA' a datetime64[ns] (sin tz)
    - Asegura 'TURNO' en int32
    - Asegura columnas numéricas en float64/int64 donde aplique
    - Convierte columnas object que no son numéricas/fechas a string
    """
    df = df.copy()
    # Fecha a datetime64[ns]
    if "FECHA" in df.columns:
        df["FECHA"] = pd.to_datetime(df["FECHA"], errors="coerce")
        try:
            df["FECHA"] = df["FECHA"].dt.tz_localize(None)
        except Exception:
            pass

    # Turno entero
    if "TURNO" in df.columns:
        df["TURNO"] = df["TURNO"].apply(clean_turno).astype("Int32").astype(int)

    # Normalizar object → string si no es fecha ni número
    for c in df.columns:
        dt = df[c].dtype
        if dt == "object":
            # Intentar numérico
            s_num = pd.to_numeric(df[c], errors="coerce")
            if s_num.notna().any() and s_num.isna().mean() < 0.5:
                df[c] = s_num
                continue
            # Intentar fecha
            s_dt = pd.to_datetime(df[c], errors="coerce")
            if s_dt.notna().any() and s_dt.isna().mean() < 0.5:
                df[c] = s_dt
                continue
            # Como texto
            df[c] = df[c].astype(str)

    return df


def strip_accents(text: str) -> str:
    """Remove diacritics (tildes) from a string using Unicode decomposition."""
    return "".join(
        c for c in unicodedata.normalize("NFKD", str(text)) if not unicodedata.combining(c)
    )
# Unificar formatos de fecha en FECHA DE PLANTACION (dd/mm/yyyy, serial Excel, datetime)
def parse_mixed_date(val):
    if pd.isna(val):
        return pd.NaT
    # Manejo de serial de Excel (número de días desde 1899-12-30)
    if isinstance(val, (int, float)):
        try:
            return pd.Timestamp("1899-12-30") + pd.Timedelta(days=int(val))
        except Exception:
            pass
    if isinstance(val, str):
        s = val.strip()
        # Si es un número en texto, tratar como serial de Excel
        if s.replace('.', '', 1).isdigit():
            try:
                return pd.Timestamp("1899-12-30") + pd.Timedelta(days=int(float(s)))
            except Exception:
                pass
        # Intentar parseo con día primero y formatos ISO
        dt = pd.to_datetime(s, dayfirst=True, errors='coerce')
        if dt is not pd.NaT:
            return dt
        return pd.NaT
    # Último intento genérico
    return pd.to_datetime(val, dayfirst=True, errors='coerce')

def format_lote(val):
    if pd.isna(val):
        return val
    s = str(val).strip()
    # Quitar prefijo existente y normalizar
    s = re.sub(r"^\s*LOTE\s+", "", s, flags=re.IGNORECASE)
    s = s.upper()
    # Corregir sufijo '-I' a '-1'
    s = re.sub(r"-I\b", "-1", s)
    s = re.sub(r"-II\b", "-2", s)
    # Separar la primera parte numérica y el resto
    parts = s.split("-")
    head = parts[0]
    tail = "-".join(parts[1:]) if len(parts) > 1 else ""
    # Zero-pad sólo la parte numérica inicial
    if head.isdigit():
        head = head.zfill(3)
    else:
        m = re.match(r"(\d+)", head)
        if m:
            head = m.group(1).zfill(3) + head[m.end():]
    return "LOTE " + head + (f"-{tail}" if tail else "")

def lacolina_transform(df = pd.DataFrame()):
    df = df.rename(columns={
        "N° LOTE": "LOTE",
        "AREA":"HA",
        "PERSONAL":"JORNAL",
        'JRS/JN':"JARRAS/JR",
        'N° JABAS' : "JABAS",
        'N° JARRAS': "JARRAS",
        'PESO BRUTO KG':"KILOS BRUTOS"

    })
    df = df.drop(columns=[
        'N° PASADA','N° JARRAS.1', 'PESO BRUTO KG.1', 'PESO NETO KG.1','% DESCARTE', 
        'N° JABAS DESCARTE','PESO NETO KG',"EMPRESA"
    ])
    #print(df.columns)
    df["FECHA"] = df["FECHA"].fillna(method='ffill')
    df["LOTE"] = df["LOTE"].fillna("x")
    df["LOTE"] = df["LOTE"].astype(str)
    df["LOTE"] = df["LOTE"].str.replace(".0","")
    df["LOTE"] = df["LOTE"].apply(format_lote)
    df["OBSERVACIONES"] = df["OBSERVACIONES"].fillna("0")
    df["OBSERVACIONES"] = df["OBSERVACIONES"].str.strip()
    df["OBSERVACIONES"] = df["OBSERVACIONES"].str.replace("TURNO MAÑANA", "1")
    df["OBSERVACIONES"] = df["OBSERVACIONES"].str.replace("TURNO TARDE", "2")
    df["OBSERVACIONES"] = df["OBSERVACIONES"].astype(int)
    df["VARIEDAD"] = df["VARIEDAD"].str.strip()
    df["FUNDO"] = "LA COLINA"
    df["MODULO"] = "M1"
    df["COD"] = "-"
    df["PACKING"] = "-"
    df["HA REAL"] = 0
    df["KG/HA REAL"] = 0
    df["DESCARTE"] = 0
    df["HA"] = df["HA"].fillna(0)
    df["JORNAL"] = df["JORNAL"].fillna(0)
    df["JABAS"] = df["JABAS"].fillna(0)
    df["JARRAS"] = df["JARRAS"].fillna(0)

    df["JARRAS/JR"] = 0
    df["KILOS /HA"] = df["KILOS BRUTOS"]/df["HA"]
    df["FECHA"] = pd.to_datetime(df["FECHA"]).dt.date
    df = df.rename(columns={"OBSERVACIONES": "TURNO"})
    return df

################################################ AGRITRACER ################################################
def time_to_decimal_hours(time_str):
    if pd.isna(time_str):
        return 0.0
    
    # Si ya es numérico (float/int), devolverlo tal cual
    if isinstance(time_str, (int, float, np.number)):
        return float(time_str)

    try:
        # Si es string
        if isinstance(time_str, str):
            # Intentar convertir string numérico directo
            try:
                return float(time_str)
            except ValueError:
                pass
            
            # Parsear formato de tiempo
            time_obj = pd.to_datetime(time_str, format='%H:%M:%S', errors='coerce')
            if pd.isna(time_obj):
                 # Intentar inferir
                 time_obj = pd.to_datetime(time_str, errors='coerce')
            
            if pd.notna(time_obj):
                 time_obj = time_obj.time()
            else:
                 return 0.0
        else:
            # Si ya es datetime o time, extraer el time
            time_obj = time_str.time() if hasattr(time_str, 'time') else time_str
        
        # Convertir a horas decimales si tiene atributos de tiempo
        if hasattr(time_obj, 'hour'):
            hours = time_obj.hour + time_obj.minute/60 + time_obj.second/3600
            return hours
        else:
            return 0.0
    except Exception:
        return 0.0


def quitar_tildes(texto):
    if pd.isna(texto):
        return texto
    # Normalizar y quitar acentos
    texto_normalizado = unicodedata.normalize('NFD', str(texto))
    texto_sin_tildes = ''.join(c for c in texto_normalizado if unicodedata.category(c) != 'Mn')
    return texto_sin_tildes

def create_fecha_final(fecha,year_campania,year_fecha):
            if year_campania == year_fecha:
                return fecha
            else:
                if year_fecha == 2023:
                    return datetime(2024, 1, 1)
                else:
                    return datetime(2024, 12, 31)
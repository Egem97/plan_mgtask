import pandas as pd
import os
import streamlit as st
import numpy as np
import re
from datetime import datetime
from utils.get_token import get_access_token
from utils.get_api import listar_archivos_en_carpeta_compartida,subir_archivo_con_reintento,get_tc_sunat_diario
from utils.helpers import get_download_url_by_name

def datos_exportacion():
        data = listar_archivos_en_carpeta_compartida(
            get_access_token(),
            "b!tHxlsZqNAU6QUMgWaE3ca52Fo5Z_2hFAulzxf54YKldI8TrtdkpnRbJDHNkfbC0L",
            "01F7DPTPRQNG2TPDCZDFF3RDSZZDIL2YPB"
        )
        export_files = [
            "Costos de Exportaciones - EXCELLENCE 2026.xlsx",
            "Costos de Exportaciones - GAP 2026.xlsx",
            "Costos de Exportaciones - QBERRIES 2026.xlsx",
            "Costos de Exportaciones - QBERRIES MAGICA 2026.xlsx",
            "Costos de Exportaciones - BIG 2026.xlsx",
            "Costos de Exportaciones - CANYON 2026.xlsx",
            "Costos de Exportaciones - TARA 2026.xlsx"
            
        ]
        data_bd_kg = pd.DataFrame()
        data_senasa = pd.DataFrame()
        data_maquila = pd.DataFrame()
        data_materi = pd.DataFrame()
        data_operador_log= pd.DataFrame()
        for file in export_files:
            url_ = get_download_url_by_name(data, file)
            bd_kg_df = pd.read_excel(url_,sheet_name="BD KG EXPORT")
            bd_kg_df["FILE"] = file
            bd_kg_df.columns = [str(c).strip().upper() for c in bd_kg_df.columns]
            bd_kg_df = bd_kg_df.rename(columns={"LINEA DE NEGOCIO":"VARIEDAD"})
            
            senasa_df = pd.read_excel(url_,sheet_name="C. SENASA")
            senasa_df["FILE"] = file
            senasa_df.columns = [str(c).strip().upper() for c in senasa_df.columns]
            senasa_df = senasa_df.rename(columns={
                "FECHA P.":"FECHA",
                "LINEA DE NEGOCIO":"VARIEDAD",
                "FORMATO ORCL":"FORMATO_ORACLE"
                }
            )
            
            maquila_df = pd.read_excel(url_,sheet_name="C. MAQUILA")
            maquila_df["FILE"] = file
            maquila_df.columns = [str(c).strip().upper() for c in maquila_df.columns]
            maquila_df = maquila_df.rename(columns={"FORMATO ORCL":"FORMATO ORCL2"})
            
            
            materi_df = pd.read_excel(url_,sheet_name="C. MATERI")
            materi_df["FILE"] = file
            materi_df.columns = [str(c).strip().upper() for c in materi_df.columns]
            materi_df = materi_df.rename(columns={
                "DESCRIPCION2":"CONCEPTO2",
                "FORMATO ORCL":"FORMATO_ORACLE",
                "LINEA DE NEGOCIO": "VARIEDAD"
                }
            )
            
            operador_log_df = pd.read_excel(url_,sheet_name="C. OPER LOG")
            operador_log_df["FILE"] = file
            operador_log_df.columns = [str(c).strip().upper() for c in operador_log_df.columns]
            operador_log_df = operador_log_df.rename(columns={"LINEA DE NEGOCIO":"VARIEDAD","CONECPTO":"CONCEPTO"})
            #concat
            data_bd_kg = pd.concat([data_bd_kg,bd_kg_df])
            data_senasa = pd.concat([data_senasa,senasa_df])
            data_maquila = pd.concat([data_maquila,maquila_df])
            data_materi = pd.concat([data_materi,materi_df])
            data_operador_log = pd.concat([data_operador_log,operador_log_df])
        return (
            data_bd_kg,
            data_senasa,
            data_maquila,
            data_materi,
            data_operador_log
        )
        
def _normalizar_columnas(df):
    """Normaliza el símbolo de número (N° / Nº -> NRO), colapsa espacios
    y elimina las columnas sin nombre (UNNAMED)."""
    df = df.copy()
    df.columns = [
        re.sub(r"\s+", " ", re.sub(r"N[°º]\s*", "NRO ", str(c))).strip()
        for c in df.columns
    ]
    # eliminar columnas sin nombre (pandas las nombra "Unnamed: N")
    df = df.drop(columns=[c for c in df.columns if c.upper().startswith("UNNAMED")])
    return df

def _mapear_presentacion(valor):
    """Deriva la PRESENTACION a partir del texto del FORMATO.
    Tipos manejados: GRANEL, 4 OZ, PINTA PLANTA, 18 OZ. Si no mapea -> OTROS."""
    if pd.isna(valor):
        return "OTROS"
    t = str(valor).upper()
    if "GRANEL" in t:
        return "GRANEL"
    if "PINTA" in t:
        return "PINTA PLANTA"
    if "18OZ" in t or "18 OZ" in t:
        return "18 OZ"
    if "4OZ" in t or "4 OZ" in t:
        return "4 OZ"
    return "OTROS"

# Mapas de renombrado por hoja. Nombres claros; las columnas de tipo fecha
# quedan marcadas explícitamente con el prefijo/sufijo FECHA_*


RENOMBRAR_BD_KG = {
    "NRO FCL": "FCL",
    "NRO PALLET": "NRO_PALLET",
    "FECHA PROD": "FECHA_PRODUCCION",
    "NOM DEL FUNDO": "FUNDO",
    "NRO DE GLOBAL GAP": "GLOBAL_GAP",
    "CD PROD": "CODIGO_PRODUCTO",
    "DESCRICPION": "DESCRIPCION",
    "KG POR CAJA": "KG_POR_CAJA",
    "FORMATO ORACLE": "FORMATO_ORACLE",
    "C.COSTOS PX": "CENTRO_COSTO_PX",
    "NRO CAJAS": "NRO_CAJAS",
    "T. KG NETO": "KG_NETO",
    "KG BRUTO": "KG_BRUTO",
    "F. DE DESP": "FECHA_DESPACHO",
    "FECHA DE ZARPE": "FECHA_ZARPE",
    "MODALIDAD DE TRANSPORTE": "MODALIDAD_TRANSPORTE",
    "PUERTO ETD": "PUERTO_ETD",
    "ACTIVIDAD PX": "ACTIVIDAD_PX",
}

RENOMBRAR_SENASA = {
    "C. COSTO": "CENTRO_COSTO",
    "FORMATO ORACLE": "FORMATO_ORACLE",
    #"LINEA DE NEGOCIO": "VARIEDAD",
    "NRO EXPEDIENTE CF": "NRO_EXPEDIENTE_CF",
    "F. ZARPE": "ESTADO_ZARPE",          # contiene el estado ("EXPORTADO"), no una fecha
    "TOTAL S/.": "TOTAL_SOLES",
    "TC": "TIPO_CAMBIO",
    "TOTAL $": "TOTAL_USD",
    "$/KG": "USD_POR_KG",
    "DESCRIPCION2": "DESCRIPCION_DETALLE",
    "PROYECTO PX": "PROYECTO_PX",
    "ACTIVIDAD PX": "ACTIVIDAD_PX",
    "EMPRESA2": "EMPRESA_2",
    "FECHA": "FECHA_DOCUMENTO",
    "NRO DOCUMENTO": "NRO_DOCUMENTO",
    "MODALIDAD DE TRANSPORTE": "MODALIDAD_TRANSPORTE",
    "PUERTO ETD": "PUERTO_ETD",
}

RENOMBRAR_MAQUILA = {
    "DESCRIPCION F": "DESCRIPCION_FORMATO",
    "FORMATO ORCL2": "FORMATO_ORACLE",
    "C. COSTO": "CENTRO_COSTO",
    "ACTIVIDAD PX": "ACTIVIDAD_PX",
    "LINEA DE NEGOCIO": "VARIEDAD",
    "C. PROYECTO": "PROYECTO_PX",
    "DESCRIPCION": "DESCRIPCION_DETALLE",
    "F. ZARPE": "FECHA_ZARPE",
    "CAJAS": "NRO_CAJAS",
    "C.UNITARIO": "COSTO_UNITARIO",
    "IMPORTE $": "IMPORTE_USD",
    "IGV $": "IGV_USD",
    "TOTAL $": "TOTAL_USD",
    "$/KG": "USD_POR_KG",
    "FECHA": "FECHA_DOCUMENTO",
    "NRO DOCUMENTO": "NRO_DOCUMENTO",
    "MODALIDAD DE TRANSPORTE": "MODALIDAD_TRANSPORTE",
    "PUERTO ETD": "PUERTO_ETD",
}

RENOMBRAR_MATERI = {
    "DESCRIPCION": "DESCRIPCION_FORMATO",
    "FORMATO ORACLE": "FORMATO_ORACLE",
    "C. COSTO PX": "CENTRO_COSTO_PX",
    "ACTIVIDAD PX": "ACTIVIDAD_PX",
    "PROYECTO PX": "PROYECTO_PX",
    "CONCEPTO2": "DESCRIPCION_DETALLE",
    "F. ZARPE": "FECHA_ZARPE",
    "C. UNITA": "COSTO_UNITARIO",
    "IMPORTE $": "IMPORTE_USD",
    "IGV $": "IGV_USD",
    "TOTAL $": "TOTAL_USD",
    "$/KG": "USD_POR_KG",
    "EMPRESA 2": "EMPRESA_2",
    "FECHA": "FECHA_DOCUMENTO",
    "NRO FACTURA": "NRO_FACTURA",
    "MODALIDAD DE TRANSPORTE": "MODALIDAD_TRANSPORTE",
    "PUERTO ETD": "PUERTO_ETD",
    #"LINEA DE NEGOCIO":"VARIEDAD"
}

RENOMBRAR_OPER_LOG = {
    "DESCRIPCION": "DESCRIPCION_FORMATO",
    "FORMATO ORCL": "FORMATO_ORACLE",
    "C. COSTO PREX": "CENTRO_COSTO_PX",
    "ACTIVIDAD PX": "ACTIVIDAD_PX",
    "PROYECTO PX": "PROYECTO_PX",
    "DESCRIPCION2": "DESCRIPCION_DETALLE",
    "F. ZARPE": "FECHA_ZARPE",
    "KILOS BRUTOS": "KILOS_BRUTOS",
    "P. UNIT": "PRECIO_UNITARIO",
    "IMPORTE $": "IMPORTE_USD",
    "IGV $": "IGV_USD",
    "TOTAL $": "TOTAL_USD",
    "$ / KG": "USD_POR_KG",
    "EMPRESA1": "EMPRESA_2",
    "FECHA": "FECHA_DOCUMENTO",
    "NRO DOCUMENTO": "NRO_DOCUMENTO",
    "MODALIDAD DE TRANSPORTE": "MODALIDAD_TRANSPORTE",
    "KILOS NETO": "KILOS_NETO",
    #"LINEA DE NEGOCIO":'VARIEDAD'
}

RENOMBRAR_VARIEDAD ={
    "MÁGICA":"MAGICA",
    "SECOYA POP":"SEKOYA POP",
    
}
# --- PRESUPUESTO (tasas USD/kg presupuestadas por unidad; hardcode) ---
# El monto es fijo por unidad y el mismo para todas las etapas de esa unidad.
PRESUPUESTO_COMEX = {
    # UNIDAD:      (maquila, materiales, operlog)
    "SAN JOSE I":  (0.55, 0.40, 0.19),
    "SAN JOSE II": (0.55, 0.38, 0.19),
    "SAN PEDRO":   (0.55, 0.38, 0.19),
    "GAP BERRIES": (0.59, 0.45, 0.19),
    "TARA FARM":   (0.61, 0.49, 0.19),
    
    "QBERRIES":    (0.56, 0.41, 0.22),

    
    "CANYON":      (0.58, 0.45, 0.19),
    "BIG BERRIES": (0.58, 0.45, 0.19),
}
# Mapeo FUNDO (como aparece en la data) -> unidad de presupuesto.
# QBERRIES = LICAPA; las etapas (p.ej. LICAPA II) comparten el mismo monto.
FUNDO_A_PRESUPUESTO = {
    "LICAPA":      "QBERRIES",
    "LICAPA II":   "QBERRIES",
    #"LICAPA II":   "QBERRIES",
    "GAP BERRIES": "GAP BERRIES",
    "SAN JOSE":    "SAN JOSE I",   # SAN JOSE I y II tienen igual monto
    "SAN PEDRO":   "SAN PEDRO",
    "SAN JOSE II":    "SAN JOSE II",
    # Pendientes de confirmar cuando tengan envíos en la data:
    "EL POTRERO": "CANYON",
    "LAS BRISAS":        "TARA FARM",
    "BIG BERRIES":        "BIG BERRIES",
}
def _norm_key(s):
    # unifica mayusculas y colapsa espacios para que el ID sea estable entre hojas
    # (evita duplicados fantasma tipo "8x18OZ" vs "8X18OZ" entre BD_KG y costos)
    return s.astype(str).str.upper().str.replace(r"\s+", " ", regex=True).str.strip()

def agregar_presupuesto(dim):
   
    ppto = pd.DataFrame(
        [(u, *v) for u, v in PRESUPUESTO_COMEX.items()],
        columns=["UNIDAD_PPTO", "PPTO_MAQUILA", "PPTO_MATERIALES", "PPTO_OPERLOG"],
    )
    dim = dim.copy()
    dim["UNIDAD_PPTO"] = dim["FUNDO"].map(FUNDO_A_PRESUPUESTO)
    dim = dim.merge(ppto, on="UNIDAD_PPTO", how="left")
    dim["UNIDAD_PPTO"] = dim["UNIDAD_PPTO"].fillna("-")
    return dim

def limpiar_comercio_exterior(df, mapa):
    """Normaliza columnas, exige ID no vacío, renombra y da formato fecha."""
    df = _normalizar_columnas(df)
    df["ID"] = df["ID"].str.strip()
    df = df[(df["ID"].notna())&(df["ID"]!="")]            # el ID no puede estar vacío
    df = df.rename(columns=mapa)
    # formato fecha a todas las columnas FECHA_* (fecha sin hora)
    cols_fecha = [c for c in df.columns if c.startswith("FECHA_")]
    for c in cols_fecha:
        df[c] = pd.to_datetime(df[c], errors="coerce").dt.normalize()
    # PRESENTACION derivada del FORMATO (si existe una columna FORMATO)
    col_formato = "FORMATO_ORACLE" if "FORMATO_ORACLE" in df.columns else next(
        (c for c in df.columns if "FORMATO" in c), None)
    if col_formato is not None:
        df["PRESENTACION"] = df[col_formato].map(_mapear_presentacion)
    # rellenar con "-" los vacíos en columnas de texto y en columnas
    # totalmente vacías (pandas las lee como float), ignorando las de FECHA
    cols_texto = [
        c for c in df.columns
        if "FECHA" not in c
        and (df[c].dtype == "object" or df[c].isna().all())
    ]
    df[cols_texto] = df[cols_texto].astype(object).fillna("-")
    ###
    
    return df.reset_index(drop=True)

def _mapa_fecha_zarpe(tablas):
    """ID -> fecha de zarpe, tomada de las tablas que sí la tienen como fecha."""
    partes = [t[["ID", "FECHA_ZARPE"]] for t in tablas if "FECHA_ZARPE" in t.columns]
    m = pd.concat(partes, ignore_index=True).dropna(subset=["FECHA_ZARPE"])
    return m.groupby("ID")["FECHA_ZARPE"].max()

def construir_dim_envio(tablas):
    #Una fila por ID. Atributos tomados de BD_KG (maestra) y completados
    #con las tablas de costo para los ID que no estén en BD_KG.
    attrs = ["ID", "FCL", "FUNDO", "EMPRESA", "AÑO", "CAMPAÑA", "PRESENTACION",
             "VARIEDAD", "MODALIDAD_TRANSPORTE", "CONTINENTE", "PUERTO_ETD", "CONTENEDOR",
             "FECHA_ZARPE"]   # fecha del envío -> conecta con Dim_Calendario (evita bucles)
    # cada tabla aporta las columnas de atributo que tenga (BD_KG primero => prioridad)
    frames = [t[[c for c in attrs if c in t.columns]] for t in tablas]
    dim = pd.concat(frames, ignore_index=True)
    # first() toma el primer valor no nulo por ID; BD_KG va primero, así que gana
    dim = dim.groupby("ID", as_index=False).first()
    # alias para que los slicers calcen con los nombres de los reportes
    dim["MERCADO"] = dim["CONTINENTE"]     # MERCADO = CONTINENTE
    dim["TEMPORADA"] = dim["CAMPAÑA"]      # TEMPORADA = CAMPAÑA
    # rellenar atributos vacíos (ID que solo vino de costos y sin dato)
    cols_txt = dim.select_dtypes(include="object").columns
    dim[cols_txt] = dim[cols_txt].fillna("-")
    dim["FCL-FUNDO"] = dim["FCL"]+"|"+ dim["EMPRESA"]

    return dim
def _preparar_df(df):
    """Deja el DataFrame listo para exportar: texto saneado.
    Se usa el MISMO resultado para el parquet local y para OneDrive."""
    df = df.copy()
    obj = df.select_dtypes(include="object").columns
    df[obj] = df[obj].astype(str).replace({"nan": "-", "NaT": "-", "None": "-"})
    return df

def update_comex():
    data_bd_kg,data_senasa,data_maquila,data_materi,data_operador_log = datos_exportacion()
    data_bd_kg["NOM DEL FUNDO"] = data_bd_kg["NOM DEL FUNDO"].fillna(data_bd_kg["ETAPA"])

    # Limpieza final: ID no vacío + columnas renombradas
    data_bd_kg        = limpiar_comercio_exterior(data_bd_kg, RENOMBRAR_BD_KG)
    data_senasa       = limpiar_comercio_exterior(data_senasa, RENOMBRAR_SENASA)
    data_maquila      = limpiar_comercio_exterior(data_maquila, RENOMBRAR_MAQUILA)
    data_materi       = limpiar_comercio_exterior(data_materi, RENOMBRAR_MATERI)
    data_operador_log = limpiar_comercio_exterior(data_operador_log, RENOMBRAR_OPER_LOG)
    data_materi.loc[data_materi["EMPRESA"] == "EXCELLENCE", "VARIEDAD"] = "SEKOYA POP"
    data_bd_kg["VARIEDAD"] = data_bd_kg["VARIEDAD"].replace(RENOMBRAR_VARIEDAD)
    data_senasa["VARIEDAD"] = data_senasa["VARIEDAD"].replace(RENOMBRAR_VARIEDAD)
    data_maquila["VARIEDAD"] = data_maquila["VARIEDAD"].replace(RENOMBRAR_VARIEDAD)
    data_materi["VARIEDAD"] = data_materi["VARIEDAD"].replace(RENOMBRAR_VARIEDAD)
    data_operador_log["VARIEDAD"] = data_operador_log["VARIEDAD"].replace(RENOMBRAR_VARIEDAD)
    data_bd_kg = data_bd_kg.drop_duplicates()
    
    data_bd_kg["ID"]        = _norm_key(data_bd_kg["ID"])        + _norm_key(data_bd_kg["VARIEDAD"])
    data_senasa["ID"]       = _norm_key(data_senasa["ID"])       + _norm_key(data_senasa["VARIEDAD"])
    data_maquila["ID"]      = _norm_key(data_maquila["ID"])      + _norm_key(data_maquila["VARIEDAD"])
    data_materi["ID"]       = _norm_key(data_materi["ID"])       + _norm_key(data_materi["VARIEDAD"])
    data_operador_log["ID"] = _norm_key(data_operador_log["ID"]) + _norm_key(data_operador_log["VARIEDAD"])
    
    _orig_zarpe = data_senasa["ESTADO_ZARPE"].astype(str).str.strip().str.upper()
    _fecha_zarpe = pd.to_datetime(data_senasa["ESTADO_ZARPE"], errors="coerce").dt.normalize()
    data_senasa["FECHA_ZARPE"] = _fecha_zarpe
    # completar las fechas faltantes (las que venían como 'EXPORTADO' o vacías)
    _mapa_zarpe = _mapa_fecha_zarpe([data_bd_kg, data_maquila, data_materi, data_operador_log])
    _falta_zarpe = data_senasa["FECHA_ZARPE"].isna()
    data_senasa.loc[_falta_zarpe, "FECHA_ZARPE"] = data_senasa.loc[_falta_zarpe, "ID"].map(_mapa_zarpe)
    # estado: conserva el texto original si es un estado; si hay fecha => EXPORTADO
    _es_estado = _fecha_zarpe.isna() & ~_orig_zarpe.isin(["-", "NAN", "NAT", "NONE", ""])
    data_senasa["ESTADO_ZARPE"] = np.where(
        _es_estado, _orig_zarpe,
        np.where(data_senasa["FECHA_ZARPE"].notna(), "EXPORTADO", "-"),
    )
    _TABLAS_COMEX = [data_bd_kg, data_senasa, data_maquila, data_materi, data_operador_log]
    dim_envio = construir_dim_envio(_TABLAS_COMEX)
    dim_envio = agregar_presupuesto(dim_envio)  
    CARPETA_PBI = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data_powerbi")
    os.makedirs(CARPETA_PBI, exist_ok=True)
    TABLAS_PBI = {
        "Fact_BDKG": data_bd_kg,
        "Fact_Senasa": data_senasa,
        "Fact_Maquila": data_maquila,
        "Fact_Materiales": data_materi,
        "Fact_OperLog": data_operador_log,
        "Dim_Envio": dim_envio

    }
    # --- Destino OneDrive (Graph) para los parquet de Power BI ---
    ONEDRIVE_DRIVE_ID  = "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s"
    ONEDRIVE_FOLDER_ID = "01KM43WT3HBEYDPDKOPFAYVUJ3JOPTX63U"
    _token = get_access_token()
    for _nombre, _df in TABLAS_PBI.items():
        _limpio = _preparar_df(_df)
        _archivo = f"{_nombre}.parquet"
        # 1) copia local (carpeta ignorada por git)
        _limpio.to_parquet(os.path.join(CARPETA_PBI, _archivo), index=False)
        # 2) subir a OneDrive con reintentos
        _ok = subir_archivo_con_reintento(
            _token, _limpio, _archivo,
            ONEDRIVE_DRIVE_ID, ONEDRIVE_FOLDER_ID, type_file="parquet",
        )
        if _ok:
            print(f"☁️ {_archivo} subido a OneDrive")
        else:
            print(f"❌ No se pudo subir {_archivo} a OneDrive (ver logs)")
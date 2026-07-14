import streamlit as st
import pandas as pd
import numpy as np
from utils.utils import *
from utils.helpers import get_download_url_by_name
from utils.get_api import listar_archivos_en_carpeta_compartida,subir_archivo_con_reintento
from utils.utils import read_excel_fast
from utils.get_token import get_access_token
from functions.agricola import data_cosecha

# Fundos que en transporte (kias/camaras) llegan agrupados bajo un solo nombre
# y deben prorratearse entre sus fundos "hijos" segun los kilos cosechados por
# fecha. El PRIMER fundo de cada lista es la "primera etapa": recibe el costo
# completo cuando esa fecha no tiene cosecha registrada del grupo.
PRORRATEO_GRUPOS = {
    "QBERRIES": ["QBERRIES I", "QBERRIES II MAGICA", "QBERRIES II SEKOYA"],
    "CANYON":   ["CANYON MAGICA", "CANYON MADEIRA"],
}

# Numeracion romana de etapas: posicion del fundo hijo dentro de su grupo.
_ETAPA_ROMANOS = ["I", "II", "III", "IV", "V"]


def _kilos_cosecha_transporte(fecha_desde="2026-06-01"):
    """KILOS BRUTOS cosechados por (FECHA, FUNDO_) para los fundos hijos de
    PRORRATEO_GRUPOS. Cacheado para no releer la cosecha en cada dataset."""
    hijos = [f for hs in PRORRATEO_GRUPOS.values() for f in hs]
    df = data_cosecha()
    df["FECHA"] = pd.to_datetime(df["FECHA"], errors="coerce").dt.normalize()
    df = df[(df["FECHA"] >= fecha_desde) & (df["FUNDO_"].isin(hijos))]
    return df.groupby(["FECHA", "FUNDO_"], as_index=False)["KILOS BRUTOS"].sum()

def _prorratear_etapas(df, fundo_col, fecha_col, cost_col, fecha_desde="2026-06-01"):
    """Reparte cost_col de los viajes de QBERRIES/CANYON entre sus fundos hijos
    SIN explotar filas: cada viaje conserva UNA fila (y su costo total intacto en
    cost_col) y se agregan columnas anchas por etapa (posicion del hijo en el grupo):

        'ETAPA {N}'        -> costo prorrateado de esa etapa (cost_col * peso)
        'KILOS ETAPA {N}'  -> KILOS BRUTOS cosechados de esa etapa en esa fecha
        'FUNDO ETAPA {N}'  -> nombre del fundo hijo que corresponde a esa etapa

    El peso es la proporcion de KILOS BRUTOS cosechados por el hijo dentro de su
    grupo en esa fecha. Si la fecha del viaje no tiene cosecha del grupo, todo el
    costo va a la ETAPA I (primer hijo). Los viajes de fundos NO agrupados quedan
    con las columnas ETAPA en 0/"" y su costo sigue integro en cost_col.

    A diferencia de la version que explotaba filas, aqui las demas columnas del
    viaje (peso, pallets, etc.) NO se duplican, evitando el doble conteo."""
    df = df.copy()
    n_max = max(len(h) for h in PRORRATEO_GRUPOS.values())
    etapas = _ETAPA_ROMANOS[:n_max]
    for r in etapas:
        df[f"ETAPA {r}"] = 0.0
        df[f"KILOS ETAPA {r}"] = 0.0
        df[f"FUNDO ETAPA {r}"] = ""

    fecha_norm = pd.to_datetime(df[fecha_col], errors="coerce").dt.normalize()
    kilos = _kilos_cosecha_transporte(fecha_desde).copy()
    kilos["FECHA"] = pd.to_datetime(kilos["FECHA"], errors="coerce").dt.normalize()

    for padre, hijos in PRORRATEO_GRUPOS.items():
        mask = (df[fundo_col] == padre) & (fecha_norm >= fecha_desde)
        if not mask.any():
            continue
        # KILOS BRUTOS por (fecha, hijo) en formato ancho: una columna por hijo.
        pk = kilos[kilos["FUNDO_"].isin(hijos)]
        kilos_w = (pk.pivot(index="FECHA", columns="FUNDO_", values="KILOS BRUTOS")
                     .reindex(columns=hijos))
        # Alinea los kilos a cada viaje del grupo por su fecha (posicional).
        f_viaje = fecha_norm[mask].to_numpy()
        k = np.nan_to_num(kilos_w.reindex(f_viaje).to_numpy(), nan=0.0)  # (n_viajes, n_hijos)
        total = k.sum(axis=1)                                           # (n_viajes,)
        con_cosecha = total > 0
        costo = df.loc[mask, cost_col].to_numpy(dtype=float)            # (n_viajes,)
        for i, hijo in enumerate(hijos):
            r = etapas[i]
            # peso = kilos_hijo / total_grupo; sin cosecha del grupo -> todo a etapa I
            peso = np.where(con_cosecha, k[:, i] / np.where(con_cosecha, total, 1.0),
                            1.0 if i == 0 else 0.0)
            df.loc[mask, f"ETAPA {r}"] = costo * peso
            df.loc[mask, f"KILOS ETAPA {r}"] = np.where(con_cosecha, k[:, i], 0.0)
            df.loc[mask, f"FUNDO ETAPA {r}"] = hijo
    return df


def desagregar_transporte(df, fecha_col, fundo_col, cost_col, monto_out):
    """Convierte el formato ancho por etapa (salida de _prorratear_etapas) a filas
    (fecha, FUNDO, monto, kilos). Los fundos agrupados (QBERRIES/CANYON) se
    desagregan en sus fundos hijos usando las columnas 'FUNDO ETAPA i'/'ETAPA i'/
    'KILOS ETAPA i'; los demas fundos aportan su costo total (cost_col) a su propio
    fundo. Devuelve columnas [fecha_col, 'FUNDO', monto_out, 'KILOS_COS'].

    Se usa en los builders para que la tabla maestra siga uniendo por fundo hijo,
    pero SIN duplicar en el parquet las demas columnas del viaje."""
    n_max = max(len(h) for h in PRORRATEO_GRUPOS.values())
    etapas = _ETAPA_ROMANOS[:n_max]
    es_grupo = df[fundo_col].isin(PRORRATEO_GRUPOS.keys())

    # Fundos no agrupados: el costo total del viaje es de su propio fundo.
    base = (df.loc[~es_grupo, [fecha_col, fundo_col, cost_col]]
              .rename(columns={fundo_col: "FUNDO", cost_col: monto_out}))
    base["KILOS_COS"] = 0.0

    partes = [base]
    grp = df.loc[es_grupo]
    for r in etapas:
        sub = grp[[fecha_col, f"FUNDO ETAPA {r}", f"ETAPA {r}", f"KILOS ETAPA {r}"]].rename(
            columns={
                f"FUNDO ETAPA {r}": "FUNDO",
                f"ETAPA {r}": monto_out,
                f"KILOS ETAPA {r}": "KILOS_COS",
            })
        # Descarta etapas vacias (p.ej. ETAPA III en CANYON, o fundos sin reparto).
        sub = sub[sub["FUNDO"].astype(str).str.len() > 0]
        partes.append(sub)
    return pd.concat(partes, ignore_index=True)

def camaras_data():
    data = listar_archivos_en_carpeta_compartida(
        get_access_token(),
        "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        "01KM43WT4M3PZTZVTV3VC3ORHLY6J4WI2X"
    )
    url_excel_1 = get_download_url_by_name(data, "APG ALZA - CONTROL T. CÁMARAS.xlsx")
    url_excel_2 = get_download_url_by_name(data, "APG TK - CONTROL DIARIO KIAS.xlsx")
    url_excel_3 = get_download_url_by_name(data, "CONTROL T. CÁMARAS ABG ALZA.xlsx")
    url_excel_4 = get_download_url_by_name(data, "APG TK - CONTROL DIARIO KIAS - 2026.xlsx")
    #APG ALZA - CONTROL T. CÁMARAS - 2026
    url_excel_5 = get_download_url_by_name(data, "APG ALZA - CONTROL T. CÁMARAS - 2026.xlsx")
    return (
        read_excel_fast(url_excel_1, sheet_name="BD", skiprows=2),
        read_excel_fast(url_excel_2, sheet_name="BD"),
        read_excel_fast(url_excel_3, sheet_name="BD", skiprows=2),
        read_excel_fast(url_excel_4, sheet_name="BD"),
        read_excel_fast(url_excel_5, sheet_name="BD", skiprows=2),
    )

def transform_camaras_kias():
    control_camaras,apg_kias,control_camaras25,apg_kias26,control_camaras26 = camaras_data()
    control_camaras["TIPO"] = "APG"
    control_camaras["CAMPAÑA"]="Campaña 2025"
    control_camaras25["TIPO"] = "COMPARTIDO"
    control_camaras25["CAMPAÑA"]="Campaña 2025"
    control_camaras25 = control_camaras25.rename(columns = {'TIPO UN.':'TIPO UNID.'})
    
    control_camaras26["TIPO"] = "APG"
    control_camaras26["CAMPAÑA"]="Campaña 2026"
    camaras_df = pd.concat([control_camaras,control_camaras25,control_camaras26])
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
       'PESO TOTAL DEL VIAJE', 'COSTO / KG', '% OCUP','TIPO','CAMPANA'
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
        
        camaras_df[cnum] = camaras_df[cnum].replace("NO REGISTRADO",0)
        camaras_df[cnum] = camaras_df[cnum].fillna(0)
        camaras_df[cnum] = camaras_df[cnum].astype(str)
        camaras_df[cnum] = camaras_df[cnum].astype(float)
        
    for coltime in cols_time:
        #camaras_df[coltime] = camaras_df[coltime].fillna('00:00:00') 
        camaras_df[coltime] = pd.to_datetime(camaras_df[coltime], format='%H:%M:%S', errors='coerce').dt.time

    camaras_df["FECHA INICIO TRASLADO"] = pd.to_datetime(camaras_df["FECHA INICIO TRASLADO"]).dt.date
    camaras_df["RUTA"] = camaras_df["RUTA"].str.split("-").str[0].str.strip()
    
    camaras_df = camaras_df.rename(columns = {"SEM":"SEMANA","CAMPANA":"CAMPAÑA"})
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
    apg_kias["CAMPAÑA"] = "Campaña 2025"
    apg_kias26.columns = (
            apg_kias26.columns.astype(str)
            .str.normalize('NFKD')
            .str.encode('ascii', errors='ignore')
            .str.decode('utf-8')
            .str.replace('\n', ' ', regex=False)
            .str.replace('.', '', regex=False)
            .str.strip()
            .str.upper()
    )
    apg_kias26["CAMPAÑA"] = "Campaña 2026"
    apg_kias = pd.concat([apg_kias,apg_kias26], axis=0)
    
    
    cols_kias =['SEMANA','FECHA', 'FUNDO', 'ACTIVIDAD', 'SERVICIO',
        'VALIDACION DETALLE DE VIAJE', 'RS FUNDO', 'PLACA',
        'PLACA REG', 'GRR', 'FACTURA', 'CONDUCTOR',
        'HORA ENTRADA', 'HORA SALIDA', 
        'CAPAC TOTAL JARRAS', 'ZONA', 'RAZON SOCIAL-PROVEEDOR',
            'PROVEEDOR-ZONA', 'TARIFA', 'TARIFA TOTAL',
        'COD TARIF PRO', 'MODULOS QBERRIES','CAMPAÑA']
    apg_kias = apg_kias[cols_kias]
    apg_kias = apg_kias[(apg_kias["FECHA"].notna())]
    cols_str_kias = ['FUNDO', 'ACTIVIDAD', 'SERVICIO',
        'VALIDACION DETALLE DE VIAJE', 'RS FUNDO', 'PLACA',
        'PLACA REG', 'FACTURA', 'CONDUCTOR',
            'ZONA', 'RAZON SOCIAL-PROVEEDOR',
            'PROVEEDOR-ZONA', 
        'COD TARIF PRO', 'MODULOS QBERRIES'
    ]
    cols_num_kias = [ 'GRR',  'CAPAC TOTAL JARRAS', 'TARIFA', 'TARIFA TOTAL']
    cols_time_kias = [
            'HORA ENTRADA', 'HORA SALIDA',
    ]
    #print(apg_kias['CAPAC TOTAL JARRAS'].unique())
    for cstrk in cols_str_kias:
            apg_kias[cstrk] = apg_kias[cstrk].fillna("-")
            apg_kias[cstrk] = apg_kias[cstrk].astype(str)
            apg_kias[cstrk] = apg_kias[cstrk].replace('ERROR','NO ESPECIFICADO')
            apg_kias[cstrk] = apg_kias[cstrk].str.strip()
            apg_kias[cstrk] = apg_kias[cstrk].str.upper()

    for cnumk in cols_num_kias:
            
            apg_kias[cnumk] = apg_kias[cnumk].fillna(0)
            apg_kias[cnumk] = apg_kias[cnumk].astype(str)
            apg_kias[cnumk] = apg_kias[cnumk].replace('ERROR','0')
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
    apg_kias["PROVEEDOR-ZONA"] = apg_kias["PROVEEDOR-ZONA"].str.replace("ERROR","")
    #.str.normalize('NFKD')

    # Prorrateo POR ETAPA (sin explotar filas): QBERRIES y CANYON llegan agrupados;
    # cada viaje conserva UNA fila y su costo total, y se agregan columnas anchas
    # 'ETAPA I/II/III' (costo prorrateado), 'KILOS ETAPA I/II/III' (kilos cosechados)
    # y 'FUNDO ETAPA I/II/III' (a que fundo hijo corresponde cada etapa).
    # Camaras -> reparte COSTO PRORRATEADO ; Kias -> reparte TARIFA.
    camaras_df = _prorratear_etapas(
        camaras_df,
        fundo_col="FUNDO PARTIDA",
        fecha_col="FECHA INICIO TRASLADO",
        cost_col="COSTO PRORRATEADO",
    )
    apg_kias = _prorratear_etapas(
        apg_kias,
        fundo_col="FUNDO",
        fecha_col="FECHA",
        cost_col="TARIFA",
    )
    return camaras_df,apg_kias

def camaras_kias_load_data():
    print(f"📤 Subiendo archivos 'CAMARA Y KIAS' a OneDrive...")
    camaras_df,kias_df =transform_camaras_kias()#, kias_df
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

def prorrateo_transporte():
    df = data_cosecha()
    df["FECHA"] = pd.to_datetime(df["FECHA"])
    df = df[df["FECHA"]>="2026-06-01"]
    df = df[df["FUNDO_"].isin(["QBERRIES II MAGICA","QBERRIES II SEKOYA","QBERRIES I"])]

    df = df.groupby(["FECHA","FUNDO_"])[["KILOS BRUTOS"]].sum().reset_index()
    df = df.pivot(index="FECHA", columns="FUNDO_", values="KILOS BRUTOS").fillna(0)
    df.columns.name = None

    kilos_cols = list(df.columns)
    df["KILOS_TOTALES"] = df[kilos_cols].sum(axis=1)
    for col in kilos_cols:
        df[f"{col}_%"] = (df[col] / df["KILOS_TOTALES"] * 100).round(2)

    df = df.reset_index()
    df = df.rename({
        'QBERRIES I':'I KG',
        'QBERRIES II MAGICA':'II KG',
        'QBERRIES II SEKOYA':'III KG',
        'QBERRIES I_%':'I %',
        'QBERRIES II MAGICA_%':'II %',
        'QBERRIES II SEKOYA_%':'III %',
    })
    st.dataframe(df)
    print(df.columns)
    camaras_df, kias_df =transform_camaras_kias()
    #QBERRIES S.A.C.
    camaras_df["FECHA INICIO TRASLADO"] = pd.to_datetime(camaras_df["FECHA INICIO TRASLADO"])
    kias_df["FECHA"] = pd.to_datetime(kias_df["FECHA"])
    camaras_df = camaras_df[(camaras_df["NOMBRE REMITENTE"]=="QBERRIES S.A.C.")&(camaras_df["FECHA INICIO TRASLADO"]>="2026-06-01")]
    camaras_df = camaras_df.rename(columns ={"FECHA INICIO TRASLADO":"FECHA"})
    kias_df = kias_df[(kias_df["FUNDO"]=="QBERRIES")&(kias_df["FECHA"]>="2026-06-01")]

    st.dataframe(camaras_df)
    st.dataframe(kias_df)

    camaras_df= pd.merge(camaras_df,df,on=["FECHA"], how="left")
    camaras_df["ETAPA I S/"] = (camaras_df["QBERRIES I_%"]/100) * camaras_df["COSTO PRORRATEADO"]
    camaras_df["ETAPA II S/"] = (camaras_df["QBERRIES II MAGICA_%"]/100) * camaras_df["COSTO PRORRATEADO"]
    camaras_df["ETAPA III S/"] = (camaras_df["QBERRIES II SEKOYA_%"]/100 )* camaras_df["COSTO PRORRATEADO"]
    st.dataframe(camaras_df)
    kias_df= pd.merge(kias_df,df,on=["FECHA"], how="left")
    kias_df["ETAPA I S/"] = (kias_df["QBERRIES I_%"]/100) * kias_df["TARIFA"]
    kias_df["ETAPA II S/"] = (kias_df["QBERRIES II MAGICA_%"]/100)  * kias_df["TARIFA"]
    kias_df["ETAPA III S/"] = (kias_df["QBERRIES II SEKOYA_%"]/100)  * kias_df["TARIFA"]
    st.dataframe(kias_df)
    kias_df.to_excel("kias_costos.xlsx",index=False)
    camaras_df.to_excel("camaras_costos.xlsx",index=False)
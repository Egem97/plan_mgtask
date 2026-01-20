from utils.helpers import get_download_url_by_name
from utils.get_api import listar_archivos_en_carpeta_compartida
from pandas.core.frame import DataFrame
from utils.get_token import get_access_token
from utils.utils import read_excel_fast
import pandas as pd


def clean_costo_laboral(df):
    df = df.loc[:, [c for c in df.columns if not str(c).strip().upper().startswith("UNNAMED")]]
    df.columns = [str(c).strip().upper() for c in df.columns]
    df = df[['FUNDO', 'FECHA', 'PARTIDA PRESUPUESTARIA', 'LABOR/ACTIVIDAD', 'HORAS','BONO COSECHA', 'BONO LABOR', 'MOVILIDAD', 'COSTO LABORAL','N° TRABAJADORES']]
    df = df[df["FECHA"].notna()]
    def fix_date(x):
        x = str(x).strip()

        # Si tiene guiones, dividimos
        if "-" in x:
            parts = x.split("-")
            
            if len(parts) == 3:
                p0 = parts[0].strip()
                p1 = parts[1].strip()
                # Handle potential time component in the last part
                p2_full = parts[2].strip()
                p2 = p2_full.split(" ")[0]
                
                # Caso M-D-Y típico → tres componentes y el año al final
                if len(p2) == 4:
                    m, d, y = p0, p1, p2
                    # Identificar M-D-Y (solo si el mes está entre 1 y 12)
                    if m.isdigit() and 1 <= int(m) <= 12:
                        return f"{d.zfill(2)}-{m.zfill(2)}-{y}"
                
                # Caso Y-D-M (Year at start)
                elif len(p0) == 4:
                    y, m, d = p0, p1, p2
                    try:
                        mi = int(m)
                        di = int(d)
                        # If m > 12, it must be Y-D-M. Swap.
                        if mi > 12:
                             return f"{y}-{d.zfill(2)}-{m.zfill(2)}"
                        # Otherwise, assume standard Y-M-D.
                        return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
                    except ValueError:
                        pass

        # Si no cumple lo anterior, devolver original
        return x
    
    #df["FECHA"] = df["FECHA"].apply(fix_date)
    df["FECHA"] = pd.to_datetime(df["FECHA"], dayfirst=True, format='mixed').dt.date
    df["HORAS"] = df["HORAS"].fillna(0)
    df["BONO COSECHA"] = df["BONO COSECHA"].fillna(0)
    
    df["BONO LABOR"] = df["BONO LABOR"].fillna(0)
    df["MOVILIDAD"] = df["MOVILIDAD"].fillna(0)
    df["COSTO LABORAL"] = df["COSTO LABORAL"].fillna(0)
    df["N° TRABAJADORES"] = df["N° TRABAJADORES"].fillna(0)

    df["FUNDO"] = df["FUNDO"].str.strip()
    df["FUNDO"] = df["FUNDO"].str.upper()
    df["PARTIDA PRESUPUESTARIA"] = df["PARTIDA PRESUPUESTARIA"].str.strip()
    df["LABOR/ACTIVIDAD"] = df["LABOR/ACTIVIDAD"].str.strip()
    df["FUNDO"] = df["FUNDO"].replace({"TARA FARM":"LAS BRISAS","CANYON":"EL POTRERO"})
    
    def clean_currency(x):
        if isinstance(x, str):
            # Remove 'S/', whitespace, and commas
            x = x.replace("S/", "").replace(",", "").strip()
            try:
                return float(x)
            except ValueError:
                return 0.0
        return x

    df["COSTO LABORAL"] = df["COSTO LABORAL"].apply(clean_currency)
    df["BONO COSECHA"] = df["BONO COSECHA"].replace(" ",0)
    df["BONO COSECHA"] = df["BONO COSECHA"].apply(clean_currency)
    df["BONO COSECHA"] = df["BONO COSECHA"].astype(float)
    df["BONO LABOR"] = df["BONO LABOR"].replace(" ",0)
    df["BONO LABOR"] = df["BONO LABOR"].apply(clean_currency)
    df["BONO LABOR"] = df["BONO LABOR"].astype(float)
    return df


def read_costo_laboral():
    access_token = get_access_token()
    files = [
        "Costo Laboral - Big MO.xlsx",
        "Costo Laboral - Canyon MO.xlsx",
        "Costo Laboral - Excellence MO.xlsx",
        "Costo Laboral - Gap MO.xlsx",
        "Costo Laboral - Golden MO.xlsx",
        "Costo Laboral - Qberries MO.xlsx",
        "Costo Laboral - Tara MO.xlsx"
    ]   
    data_general = pd.DataFrame()
    for file in files:
        data = listar_archivos_en_carpeta_compartida(
            access_token,
            "b!nQ5Z090m9keqA8S99a5Wa_gI_FmonXtKlhzUKGbAKlx_NGWVoo3wRozum3uuwV37",
            "01KQPAXGDCQ4ABNVNFLBGKR3EIGQWHJQBK"
        )
        
        url_excel= get_download_url_by_name(data, file)
        
        df = read_excel_fast(url_excel)
        
        df = clean_costo_laboral(df)
        #import streamlit as st
        #st.write(file)
        data_general = pd.concat([data_general,df],axis=0)
    return data_general
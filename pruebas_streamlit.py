import streamlit as st
import numpy as np
from functions.agricola import poligonos,informe_plantacion,aplicativoNutricional
from functions.load_onedrive import fertiriego_load_data,cosecha_load_data
from functions.workup import costos_cosecha
from utils.get_token import get_access_token


st.set_page_config(page_title="web pruebas", page_icon=":tada:")
st.title("web pruebas")
access_token = get_access_token()


df = costos_cosecha()
df["VENTA EXPORTACION"] = np.where(df["PACKING"] == "ALZA PERU PACKING", df["KILOS BRUTOS"], 0)
df["VENTA NACIONAL"] = np.where(df["PACKING"] == "VENTA NACIONAL", df["KILOS BRUTOS"], 0)
st.write(df.shape)
st.dataframe(df)
dff = df.groupby(["FUNDO","PACKING"])[["KILOS BRUTOS","VENTA EXPORTACION","VENTA NACIONAL"]].sum().reset_index()
st.dataframe(dff)
data = df.groupby(["FECHA","FUNDO"])[["KILOS BRUTOS","VENTA EXPORTACION","VENTA NACIONAL","DESCARTE"]].sum().reset_index()
data = data[data["FUNDO"] == "SAN JOSE II"]

st.write(data.shape)
st.dataframe(data)

#fertiriego_load_data(access_token)
#st.success("SE REALIZO EL PROCESO DE FERTIRRIEGO")
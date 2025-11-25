import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import os
import re
from utils.utils import *

from utils.helpers import get_download_url_by_name
from utils.get_api import listar_archivos_en_carpeta_compartida
from pandas.core.frame import DataFrame
from utils.get_token import get_access_token


def costos_cosecha(access_token):
    data = listar_archivos_en_carpeta_compartida(
        access_token,
        "b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR",
        "01XOBWFSDI34HN5SD7HBHZRUBPX3457IPQ"
    )
    url_excel_1 = get_download_url_by_name(data, "COSECHA CAMPO.parquet")
    return pd.read_parquet(url_excel_1)
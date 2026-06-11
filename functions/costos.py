import streamlit as st
import pandas as pd
from utils.utils import *
from utils.helpers import get_download_url_by_name
from utils.get_api import listar_archivos_en_carpeta_compartida,subir_archivo_con_reintento
from utils.utils import read_excel_fast
from utils.get_token import get_access_token
from utils.get_kiss import fetch_all_kissflow
from utils.helpers import calcular_semana_anclada_enero

def planes_trabajo_data():
    data = listar_archivos_en_carpeta_compartida(
        get_access_token(),
        "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        "01KM43WT4OE5STC3Y275A3F3U2KF2OLQCS"
    )
    url_excel_1 = get_download_url_by_name(data, "Registros Kissflow - Planes de trabajo.xlsx")
    actividad = read_excel_fast(url_excel_1, sheet_name="Hoja1")
    insumos = read_excel_fast(url_excel_1, sheet_name="HOJA 2")
    return (actividad,insumos)

def plt_costos_insumos():
    data = listar_archivos_en_carpeta_compartida(
        get_access_token(),
        "b!DKrRhqg3EES4zcUVZUdhr281sFZAlBZDuFVNPqXRguBl81P5QY7KRpUL2n3RaODo",
        "01PNBE7BASRPKOTFOOCVFLH2B57MZ5K35S"
    )
    url_excel_1 = get_download_url_by_name(data, "Insumos Canyon Berries.xlsx")
    url_excel_2 = get_download_url_by_name(data, "Insumos Excellence Fruit.xlsx")
    url_excel_3 = get_download_url_by_name(data, "Insumos Gap Berries.xlsx")
    url_excel_4 = get_download_url_by_name(data, "Insumos Qberries.xlsx")
    url_excel_5 = get_download_url_by_name(data, "Insumos Tara Farms.xlsx")
    
    insumos_canyon = read_excel_fast(url_excel_1, sheet_name="Hoja1")
    insumos_excellence = read_excel_fast(url_excel_2, sheet_name="Hoja1")
    insumos_gap = read_excel_fast(url_excel_3, sheet_name="Hoja1")
    insumos_qberries = read_excel_fast(url_excel_4, sheet_name="Hoja1")
    insumos_tara = read_excel_fast(url_excel_5, sheet_name="Hoja1")
    df = pd.concat([insumos_canyon,insumos_excellence,insumos_gap,insumos_qberries,insumos_tara], ignore_index=True)
    return df

def plt_costos_actividades():
    data = listar_archivos_en_carpeta_compartida(
        get_access_token(),
        "b!DKrRhqg3EES4zcUVZUdhr281sFZAlBZDuFVNPqXRguBl81P5QY7KRpUL2n3RaODo",
        "01PNBE7BASRPKOTFOOCVFLH2B57MZ5K35S"
    )
    url_excel_1 = get_download_url_by_name(data, "Insumos Canyon Berries.xlsx")
    url_excel_2 = get_download_url_by_name(data, "Insumos Excellence Fruit.xlsx")
    url_excel_3 = get_download_url_by_name(data, "Insumos Gap Berries.xlsx")
    url_excel_4 = get_download_url_by_name(data, "Insumos Qberries.xlsx")
    url_excel_5 = get_download_url_by_name(data, "Insumos Tara Farms.xlsx")
    
    insumos_canyon = read_excel_fast(url_excel_1, sheet_name="Hoja1")
    insumos_excellence = read_excel_fast(url_excel_2, sheet_name="Hoja1")
    insumos_gap = read_excel_fast(url_excel_3, sheet_name="Hoja1")
    insumos_qberries = read_excel_fast(url_excel_4, sheet_name="Hoja1")
    insumos_tara = read_excel_fast(url_excel_5, sheet_name="Hoja1")
    df = pd.concat([insumos_canyon,insumos_excellence,insumos_gap,insumos_qberries,insumos_tara], ignore_index=True)
    return df
#def transform_


def transform_plt():
    """
    actividad_df,insumos_df = planes_trabajo_data()
    actividad_df.columns = (
                actividad_df.columns.astype(str)
                .str.normalize('NFKD')
                .str.encode('ascii', errors='ignore')
                .str.decode('utf-8')
                .str.replace('\n', ' ', regex=False)
                .str.replace('.', '', regex=False)
                .str.strip()
                .str.upper()
    )
    insumos_df.columns = (
                insumos_df.columns.astype(str)
                .str.normalize('NFKD')
                .str.encode('ascii', errors='ignore')
                .str.decode('utf-8')
                .str.replace('\n', ' ', regex=False)
                .str.replace('.', '', regex=False)
                .str.strip()
                .str.upper()
    )
    """
    actividad_df = fetch_all_kissflow("COP01_BDRG_JORNALES_PLANES_DE_TRABAJO")
    actividad_df = actividad_df.rename(columns={'FECHAC_DE_INICIO':'FECHA','SUB_AREA':'SUBAREA','ANO':'YEAR'})#,'TOTAL $':'TOTAL'
    actividad_df["FECHA"] = pd.to_datetime(actividad_df["FECHA"]).dt.date
    for col_num in ["YEAR","SEMANA","FACTOR","JORNALES","TOTAL"]:
        actividad_df[col_num] = actividad_df[col_num].fillna(0)
        #actividad_df[col_num] = actividad_df[col_num].astype(str)
        actividad_df[col_num] = actividad_df[col_num].astype(float)
        
    for col_str in ["FUNDO","AREA","SUBAREA","ACTIVIDAD","TIPO"]:    
        actividad_df[col_str] = actividad_df[col_str].fillna("-")
        actividad_df[col_str] = actividad_df[col_str].astype(str)
        actividad_df[col_str] = actividad_df[col_str].str.strip()
        actividad_df[col_str] = actividad_df[col_str].str.upper()

    actividad_df["FUNDO"] = actividad_df["FUNDO"].fillna("NO ESPECIFICADO")
    actividad_df["FUNDO"] = actividad_df["FUNDO"].replace({
        "GAP": "GAP BERRIES",
        "CANYON BERRIES":"EL POTRERO",
        "LICAPA I":"LICAPA",
        "SAN JOSE I":"SAN JOSE",
        "SAN JOSEII":"SAN JOSE II"
    })
    actividad_df["ACTIVIDAD"] = actividad_df["ACTIVIDAD"].apply(quitar_tildes)
    actividad_df["ACTIVIDAD"] = actividad_df["ACTIVIDAD"].replace(
        {   
            "SUPEVISOR DE FITOSANIDAD":"SUPERVISOR DE FITOSANIDAD"
        }
    )
    #actividad_df["ACTIVIDAD"] = actividad_df["ACTIVIDAD"].replace({
        
    #})
    #print(actividad_df["ACTIVIDAD"].unique())
    ################################
    insumos_df = fetch_all_kissflow("COP01_BD_PLANES_DE_TRABAJO")
    
    insumos_df = insumos_df.rename(columns={'ANO':'YEAR','FECHAC_DE_INICIO':'FECHA'})
    insumos_df["FECHA"] = pd.to_datetime(insumos_df["FECHA"]).dt.date
    
    for col_num in ["YEAR","SEMANA","FACTOR","CANTIDAD","TOTAL"]:
        insumos_df[col_num] = insumos_df[col_num].fillna(0)
        insumos_df[col_num] = insumos_df[col_num].replace("",0)
        insumos_df[col_num] = insumos_df[col_num].astype(float)
    
    for col_str in ["FUNDO","INSUMO","VARIEDAD","TIPO"]:    
        insumos_df[col_str] = insumos_df[col_str].fillna("-")
        insumos_df[col_str] = insumos_df[col_str].astype(str)
        insumos_df[col_str] = insumos_df[col_str].str.strip()
        insumos_df[col_str] = insumos_df[col_str].str.upper()
    insumos_df["FUNDO"] = insumos_df["FUNDO"].fillna("NO ESPECIFICADO")
    insumos_df["FUNDO"] = insumos_df["FUNDO"].replace({
        "GAP": "GAP BERRIES",
        "CANYON BERRIES":"EL POTRERO",
        "LICAPA I":"LICAPA",
        "SAN JOSE I":"SAN JOSE",
        "SAN JOSEII":"SAN JOSE II"
    })
    return (actividad_df,insumos_df)

def plt_load_data():
    print(f"📤 Subiendo archivos 'PLANES DE TRABAJO' a OneDrive...")
    actividad_df, insumos_df =transform_plt()
    resultado_1 = subir_archivo_con_reintento(
        access_token=get_access_token(),
        dataframe=actividad_df,
        nombre_archivo="PLT_ACTIVIDADES.parquet",
        drive_id="b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        folder_id="01KM43WT4FS6JNXKKHRNCJZMAXLX56IOEQ",
        type_file="parquet"
    )
    if resultado_1:
        print(f"✅ Proceso 1 completado exitosamente")
    resultado_2 = subir_archivo_con_reintento(
        access_token=get_access_token(),
        dataframe=insumos_df,
        nombre_archivo="PLT_INSUMOS.parquet",
        drive_id="b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        folder_id="01KM43WT4FS6JNXKKHRNCJZMAXLX56IOEQ",
        type_file="parquet"
    )
    if resultado_2:
        print(f"✅ Proceso 2 completado exitosamente")
    else:
        print(f"❌ Error al subir el archivo")
        
        
####PLANES DE TRABAJO
def plt_historico_act(token):
    data = listar_archivos_en_carpeta_compartida(
        token,
        "b!DKrRhqg3EES4zcUVZUdhr281sFZAlBZDuFVNPqXRguBl81P5QY7KRpUL2n3RaODo",
        "01PNBE7BAO4DGWZJMGY5FJKAD2FLCSEEVW"
    )
    url_ = get_download_url_by_name(data, "BD PLAN TRABAJO.xlsx")
    plt_ppt_df = pd.read_excel(url_,sheet_name=".PPTO.")
    plt_ejec_df = pd.read_excel(url_,sheet_name="EJECUTADO")
    indice_df = pd.read_excel(url_,sheet_name="BASE APOYO")
    return plt_ppt_df,plt_ejec_df,indice_df

def agritracer_(token):
    data = listar_archivos_en_carpeta_compartida(
        token,
        "b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR",
        "01XOBWFSG7XR5BMRB6XNAJBU7Q7KXU7BO3"
    )
    url_ = get_download_url_by_name(data, "AGRITRACER_GENERAL.parquet")
    df = pd.read_parquet(url_)
    
    return df
def plt_kissflow(token):
    data = listar_archivos_en_carpeta_compartida(
        token,
        "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        "01KM43WT4FS6JNXKKHRNCJZMAXLX56IOEQ"
    )
    url_ = get_download_url_by_name(data, "PLT_ACTIVIDADES.parquet")
    df = pd.read_parquet(url_)
    
    return df

def PLT_CORE_():
    token = get_access_token()
    agritracer_df = agritracer_(token)
    actividad_df = plt_kissflow(token)
    plt_ppt_df,plt_ejec_df,index_df = plt_historico_act(token)
    ################# agritracer
    agritracer_df["SEMANA"] = agritracer_df["FECHA"].apply(calcular_semana_anclada_enero).astype("Int64")
    agritracer_df["FECHA"] = pd.to_datetime(agritracer_df["FECHA"], errors="coerce")
    agritracer_df["YEAR"] = agritracer_df["FECHA"].dt.year
    agritracer_df["PERIODO SEMANA"] = agritracer_df["SEMANA"].astype(str)+"-"+agritracer_df["YEAR"].astype(str)
    agritracer_df = agritracer_df.groupby(["FUNDO","YEAR","FECHA","PERIODO SEMANA","SEMANA","ACTIVIDAD","PARTIDA PRESUPUESTARIA","MACRO PARTIDA","TRABAJADOR"])[["COPIA"]].count().reset_index()
    agritracer_df = agritracer_df.groupby(["FUNDO","YEAR","SEMANA","PERIODO SEMANA","ACTIVIDAD","PARTIDA PRESUPUESTARIA","MACRO PARTIDA"])[["COPIA"]].count().reset_index()
    agritracer_df["PERIODO SEMANA"] = agritracer_df["PERIODO SEMANA"].astype(str)
    agritracer_df = agritracer_df.rename(columns={"COPIA":"JORNALES_AGRITRACER"})
    agritracer_df["ACTIVIDAD"] = agritracer_df["ACTIVIDAD"].replace({
        'MO POLINIZACION':'POLINIZACION',
        'AUXILAR DE RIEGO':'AUXILIAR DE RIEGO',
        'RASTRILLADO DE  CAMPO':'RASTRILLADO DE CAMPO'
        
    })
    print("lista agri")
    print(agritracer_df["ACTIVIDAD"].unique())
    AGRI_SEMANA_MAX = agritracer_df[agritracer_df["YEAR"]==2026]["SEMANA"].max()
    # planes de trabajo jornales
    actividad_df["COSTO PROYECTADO"] = actividad_df["FACTOR"] * actividad_df["JORNALES"]
    actividad_df = actividad_df.rename(columns={"JORNALES":"JORNALES_PROYECTADOS"})
    actividad_df = actividad_df.drop(columns=["TIPO","TOTAL","FACTOR"])
    #fecha_max_semana = actividad_df.groupby(['YEAR', 'SEMANA', 'FUNDO','ACTIVIDAD'  ])["FECHA"].transform("max")
    #st.dataframe(actividad_df)
    #actividad_df = actividad_df[actividad_df["FECHA"] == fecha_max_semana]
    """"""
    GRANO = ["SEMANA", "FUNDO", "AREA", "SUBAREA", "ACTIVIDAD"]
    max_fecha = actividad_df.groupby(GRANO)["FECHA"].transform("max")
    actividad_df = actividad_df[actividad_df["FECHA"] == max_fecha].copy()
    
    resumen = (
        actividad_df.groupby("SEMANA")
        .agg(PROY_JR=("JORNALES_PROYECTADOS", "sum"), FILAS=("COSTO PROYECTADO", "size"))
        .reset_index()
    )
    """"""
    actividad_df = actividad_df.groupby(['YEAR', 'SEMANA', 'FUNDO', 'AREA', 'SUBAREA', 'ACTIVIDAD'])[['JORNALES_PROYECTADOS', 'COSTO PROYECTADO']].sum().reset_index()
    actividad_df["ACTIVIDAD"] = actividad_df["ACTIVIDAD"].replace(
            {   
                "SUPEVISOR DE FITOSANIDAD":"SUPERVISOR DE FITOSANIDAD",
                "AUXILAR DE RIEGO":"AUXILIAR DE RIEGO",
                "RASTRILLADO DE  CAMPO":"RASTRILLADO DE CAMPO"
            }
        )
    
    jornales_proy_excedentes_df = actividad_df[(actividad_df["YEAR"]==2026)&(actividad_df["SEMANA"]>int(AGRI_SEMANA_MAX))]
    jornales_proy_excedentes_df["PERIODO SEMANA"] = jornales_proy_excedentes_df["SEMANA"].astype(int).astype(str)+"-"+jornales_proy_excedentes_df["YEAR"].astype(int).astype(str)
    actividad_df = actividad_df[(actividad_df["YEAR"]==2026)&(actividad_df["SEMANA"]<=int(AGRI_SEMANA_MAX))]
    #st.dataframe(actividad_df)
    #
    print("lista kissflow jornales")
    print(actividad_df["ACTIVIDAD"].unique())
    plt_ejec_df.columns = (
            plt_ejec_df.columns.astype(str)
            .str.normalize('NFKD')
            .str.encode('ascii', errors='ignore')
            .str.decode('utf-8')
            .str.replace('\n', ' ', regex=False)
            .str.replace('.', '', regex=False)
            .str.strip()
            .str.upper()
    )
    plt_ejec_df["FUNDO"] = plt_ejec_df["FUNDO"].str.upper()
    plt_ejec_df["FUNDO"] = plt_ejec_df["FUNDO"].str.strip()
    plt_ejec_df["FUNDO"] = plt_ejec_df["FUNDO"].replace({
        'CANYON BERRIES':'EL POTRERO',
        'SAN JOSE I':'SAN JOSE',
        'TARA FARM':'LAS BRISAS',
        'QBERRIES I':'LICAPA',
        'QBERRIES II':'LICAPA II',
        'QBERRIES III':'LICAPA III',
        
    })
    plt_ejec_df = plt_ejec_df[(plt_ejec_df["FECHA"].notna())&(plt_ejec_df["FECHA"]!="SALDO INICIAL")&(plt_ejec_df["YEAR"]==2026)]
    plt_ejec_df["JORNALES"] = plt_ejec_df["JORNALES"].fillna(0)
    plt_ejec_df["COSTO"] = plt_ejec_df["COSTO"].fillna(0)
    plt_ejec_df["ACTIVIDAD"] = plt_ejec_df["ACTIVIDAD"].apply(quitar_tildes)
    plt_ejec_df = plt_ejec_df.drop(columns=["TIPO"])
    plt_ejec_df = plt_ejec_df.groupby(['YEAR', 'SEMANA', 'FUNDO', 'AREA', 'SUBAREA', 'ACTIVIDAD'])[['JORNALES', 'COSTO']].sum().reset_index()
    hist_eject_df = plt_ejec_df[plt_ejec_df["SEMANA"]<=19]
    hist_eject_df = hist_eject_df.rename(columns={"JORNALES":"JORNALES_PROYECTADOS","COSTO":"COSTO PROYECTADO"})
    hist_eject_df["COSTO EJECUTADO"] = hist_eject_df["COSTO PROYECTADO"]
    st.write("ejecutados menores a la semana 19")
    st.dataframe(hist_eject_df)
    costos_ejecutados = plt_ejec_df[plt_ejec_df["SEMANA"]>19]
    costos_ejecutados = costos_ejecutados.drop(columns=["JORNALES","AREA","SUBAREA"])
    costos_ejecutados = costos_ejecutados.rename(columns={"COSTO":"COSTO EJECUTADO"})
    st.write("ejecutados")
    st.dataframe(costos_ejecutados)
    #JOINS "CONCAT HISTORICA-KISSFLOW PLT"
    plt_proyectados_jr = pd.concat([hist_eject_df,actividad_df], ignore_index=True)
    st.write("ejecutados join historico vs actividad")
    st.dataframe(plt_proyectados_jr)
    #JOINS "CONCAT MERGE PLT - JR EJECUTADOS(AGRITRACER)"
    dataframe_join_1 = pd.merge(agritracer_df,plt_proyectados_jr,on=['YEAR', 'SEMANA', 'FUNDO','ACTIVIDAD',], how="outer")
    
    st.dataframe(dataframe_join_1)
    #JOINS "ERGE JR general - COSTOS (semana 20>=)"
    dataframe_join_1 = pd.merge(dataframe_join_1,costos_ejecutados,on=['YEAR', 'SEMANA', 'FUNDO','ACTIVIDAD',], how="left", suffixes=("_HIST", "_EJEC"))
    # Coalescer las dos columnas de COSTO EJECUTADO que genera el merge:
    # _EJEC (costos_ejecutados, semana > 19) tiene prioridad; si falta, usa _HIST (historico, semana <= 19)
    dataframe_join_1["COSTO EJECUTADO"] = dataframe_join_1["COSTO EJECUTADO_EJEC"].fillna(dataframe_join_1["COSTO EJECUTADO_HIST"])
    dataframe_join_1 = dataframe_join_1.drop(columns=["COSTO EJECUTADO_HIST", "COSTO EJECUTADO_EJEC"])

    dataframe_join_1 = pd.concat([dataframe_join_1,jornales_proy_excedentes_df],axis=0)
    dataframe_join_1["JORNALES_AGRITRACER"] = dataframe_join_1["JORNALES_AGRITRACER"].fillna(0)
    dataframe_join_1["COSTO EJECUTADO"] = dataframe_join_1["COSTO EJECUTADO"].fillna(0)
    dataframe_join_1["COSTO PROYECTADO"] = dataframe_join_1["COSTO PROYECTADO"].fillna(0)
    dataframe_join_1["JORNALES_PROYECTADOS"] = dataframe_join_1["JORNALES_PROYECTADOS"].fillna(0)
    plt_ppt_df.columns = (
        plt_ppt_df.columns.astype(str)
        .str.normalize('NFKD')
        .str.encode('ascii', errors='ignore')
        .str.decode('utf-8')
        .str.replace('\n', ' ', regex=False)
        .str.replace('.', '', regex=False)
        .str.strip()
        .str.upper()
    )
    plt_ppt_df["FUNDO"] = plt_ppt_df["FUNDO"].str.upper()
    plt_ppt_df["FUNDO"] = plt_ppt_df["FUNDO"].str.strip()
    plt_ppt_df["FUNDO"] = plt_ppt_df["FUNDO"].replace({
        'CANYON BERRIES':'EL POTRERO',
        'SAN JOSE I':'SAN JOSE',
        'TARA FARMS':'LAS BRISAS',
        'QBERRIES I':'LICAPA',
        'QBERRIES II':'LICAPA II',
        'QBERRIES III':'LICAPA III',
        
    })
    plt_ppt_df["LABOR"] = plt_ppt_df["LABOR"].str.upper()
    plt_ppt_df["LABOR"] = plt_ppt_df["LABOR"].str.strip()

    plt_ppt_df["AREA"] = plt_ppt_df["AREA"].str.upper()
    plt_ppt_df["AREA"] = plt_ppt_df["AREA"].str.strip()
    plt_ppt_df["LABOR"] = plt_ppt_df["LABOR"].apply(quitar_tildes)

    plt_ppt_df = plt_ppt_df.rename(columns={'PESTANA':'ORIGEN','COSTO (USD)':'COSTO PRESUPUESTO','LABOR':'ACTIVIDAD'})
    plt_ppt_df["AñoMes"] = ("2026" + plt_ppt_df["NUM MES"].astype(int).astype(str).str.zfill(2)).astype(int)
    #
    index_df["ACTIVIDAD"] = index_df["ACTIVIDAD"].str.upper()
    index_df["ACTIVIDAD"] = index_df["ACTIVIDAD"].str.strip()
    index_df["ACTIVIDAD"] = index_df["ACTIVIDAD"].apply(quitar_tildes)
    index_df["AREA"] = index_df["AREA"].str.upper()
    index_df["AREA"] = index_df["AREA"].str.strip()
    
    
    
    
    dataframe_join_1['PERIODO SEMANA'] = dataframe_join_1["SEMANA"].astype(int).astype(str)+"-"+dataframe_join_1["YEAR"].astype(int).astype(str)
    return dataframe_join_1,plt_ppt_df,index_df

def PLT_ACTIVIDADES_GENERAL():
    print(f"📤 Subiendo archivos 'ACT - PLANES DE TRABAJO' a OneDrive...")
    dataframe_join_1,plt_ppt_df,index_df = PLT_CORE_()
    resultado_1 = subir_archivo_con_reintento(
        access_token=get_access_token(),
        dataframe=dataframe_join_1,
        nombre_archivo="PLT_ACTIVIDADES_GENERAL.parquet",
        drive_id="b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        folder_id="01KM43WT4FS6JNXKKHRNCJZMAXLX56IOEQ",
        type_file="parquet"
    )
    if resultado_1:
        print(f"✅ Proceso 1 completado exitosamente")
    resultado_2 = subir_archivo_con_reintento(
        access_token=get_access_token(),
        dataframe=plt_ppt_df,
        nombre_archivo="PPTO_JORNALES_COSTOS.parquet",
        drive_id="b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        folder_id="01KM43WT4FS6JNXKKHRNCJZMAXLX56IOEQ",
        type_file="parquet"
    )
    if resultado_2:
        print(f"✅ Proceso 2 completado exitosamente")
    else:
        print(f"❌ Error al subir el archivo")
        
    
    resultado_3 = subir_archivo_con_reintento(
        access_token=get_access_token(),
        dataframe=index_df,
        nombre_archivo="PLT_JR_MAPPING.parquet",
        drive_id="b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        folder_id="01KM43WT4FS6JNXKKHRNCJZMAXLX56IOEQ",
        type_file="parquet"
    )
    if resultado_3:
        print(f"✅ Proceso 3 completado exitosamente")
    else:
        print(f"❌ Error al subir el archivo")
        

def redimientos_ppto_costos():
    print(f"📤 Subiendo archivos 'ACT - PLANES DE TRABAJO' a OneDrive...")
    def cosecha_ppto_():
        data = listar_archivos_en_carpeta_compartida(
            get_access_token(),
            "b!SVsG7KxV3EShgK6A6P5nH8F5fySlAsRHnUuG-SGbK8wEoIXhn_g4T4PoBqMjb_xH",
            "016WMGJCUL63W2LEO4JVE2ABL7PVCROI4O"
        )
        url_excel_1 = get_download_url_by_name(data, "1. Costos Cosecha.xlsx")
        rendimientos_df = read_excel_fast(url_excel_1, sheet_name="RENDIMIENTOS PPTO")
        kg_formula_df = read_excel_fast(url_excel_1, sheet_name="KG PLANTA")
        return (rendimientos_df,kg_formula_df)
    rendimientos_df,kg_formula_df = cosecha_ppto_()
    rendimientos_df["FUNDO"] = rendimientos_df["FUNDO"].str.strip()
    kg_formula_df["FUNDO"] = kg_formula_df["FUNDO"].str.strip()
    df = pd.merge(rendimientos_df,kg_formula_df, on=["FUNDO","CAMPAÑA"], how="inner")
    df = df[df["CAMPAÑA"]=="CAMPAÑA 2026"]
    resultado_1 = subir_archivo_con_reintento(
        access_token=get_access_token(),
        dataframe=df,
        nombre_archivo="PPTO_RENDIMIENTOS.parquet",
        drive_id="b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        folder_id="01KM43WT4FS6JNXKKHRNCJZMAXLX56IOEQ",
        type_file="parquet"
    )
    if resultado_1:
        print(f"✅ Proceso 3 completado exitosamente")
    else:
        print(f"❌ Error al subir el archivo")
        
def datos_transporte_2026_():
        data = listar_archivos_en_carpeta_compartida(
            get_access_token(),
            "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
            "01KM43WT4M3PZTZVTV3VC3ORHLY6J4WI2X"
        )
        url_excel_1 = get_download_url_by_name(data, "APG TK - CONTROL DIARIO KIAS - 2026.xlsx")
        df = read_excel_fast(url_excel_1, sheet_name="BD")
        
        return df
    
def datos_tipo_cambio_():
        data = listar_archivos_en_carpeta_compartida(
            get_access_token(),
            "b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR",
            "01XOBWFSDI34HN5SD7HBHZRUBPX3457IPQ"
        )
        url_excel_1 = get_download_url_by_name(data, "TIPO DE CAMBIO.xlsx")
        df = read_excel_fast(url_excel_1)    
        return df 
###### costos
def datos_transporte_personal():
        data = listar_archivos_en_carpeta_compartida(
            get_access_token(),
            "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
            "01KM43WT25M5RFQXKLAVHLRU2SMBZSTYEE"
        )
        url_excel_1 = get_download_url_by_name(data, "1. TRANSPORTE PERSONAL 2026.xlsx")
        df = read_excel_fast(url_excel_1,sheet_name="CUADRO RESUMEN")    
        return df 
    
def costos_cosecha_2026():
    def datos_cosecha_2026_():
        data = listar_archivos_en_carpeta_compartida(
            get_access_token(),
            "b!SVsG7KxV3EShgK6A6P5nH8F5fySlAsRHnUuG-SGbK8wEoIXhn_g4T4PoBqMjb_xH",
            "016WMGJCUL63W2LEO4JVE2ABL7PVCROI4O"
        )
        url_excel_1 = get_download_url_by_name(data, "1. Costos Cosecha.xlsx")
        df = read_excel_fast(url_excel_1, sheet_name="QBERRIES II MAGICA")
        
        return df
    transp_df = datos_transporte_personal()
    df = datos_cosecha_2026_()
    df.columns = (
            df.columns.astype(str)
            .str.normalize('NFKD')
            .str.encode('ascii', errors='ignore')
            .str.decode('utf-8')
            .str.replace('\n', ' ', regex=False)
            .str.replace('.', '', regex=False)
            .str.strip()
            .str.upper()
    )
    df = df[[
        'CAMPANA', 'SEMANA', 'FECHA', 'VARIEDAD', 'GRUPO COSECHA',
        'COSTO MO S/ GRUPO COSECHA', 'GRUPO  COSECHA DOLAR',
        'COSTO MANO DE OBR SOLO COSECHA', 'JORNALES', 'DISAL',
        'ALQUILER DE BANOS  VU DIARIO S/', 'COSTO DISAL DIARIO', 'COSTO DIS  $',
        'KG MATERIA PRIMA A PACKING', 'COMERCIAL DE HUERTO BUENO',
        'COMERCIAL DE HUERTO', 'KG COSECHADOS BRUTO', 'JARRAS', 'HA',
        '% COMERCIAL DE HUERTO', 'KG BRUTO/JR', 'JARRAS /JR', 'KG BRUTO /JARRA',
        'EPPS (POLOS, GORRAS, MASCARILLAS, ETC)', 'SUMINISTROS ACOPIO', 'OTROS',
        'ACTIVIDADES Y OTROS COSECHA (RRHH)'
    ]]

    col_numericos=[
        'GRUPO COSECHA',
        'COSTO MO S/ GRUPO COSECHA', 'GRUPO  COSECHA DOLAR',
        'COSTO MANO DE OBR SOLO COSECHA', 'JORNALES', 'DISAL',
        'ALQUILER DE BANOS  VU DIARIO S/', 'COSTO DISAL DIARIO', 'COSTO DIS  $',
        'KG MATERIA PRIMA A PACKING', 'COMERCIAL DE HUERTO BUENO',
        'COMERCIAL DE HUERTO', 'KG COSECHADOS BRUTO', 'JARRAS', 'HA',
        '% COMERCIAL DE HUERTO', 'KG BRUTO/JR', 'JARRAS /JR', 'KG BRUTO /JARRA',
        'EPPS (POLOS, GORRAS, MASCARILLAS, ETC)', 'SUMINISTROS ACOPIO', 'OTROS',
        'ACTIVIDADES Y OTROS COSECHA (RRHH)'
    ]

    df = df.rename(columns={"CAMPANA":"CAMPAÑA"})
    df["FECHA"] = pd.to_datetime(df["FECHA"], errors="coerce")

    
            
    for col_str in ["CAMPAÑA","VARIEDAD"]:    
            df[col_str] = df[col_str].fillna("-")
            df[col_str] = df[col_str].astype(str)
            df[col_str] = df[col_str].str.strip()
            df[col_str] = df[col_str].str.upper()
    df["FUNDO"] = "QBERRIES II MAGICA"
    
    transport_df = datos_transporte_2026_()
    transport_df.columns = (
                transport_df.columns.astype(str)
                .str.normalize('NFKD')
                .str.encode('ascii', errors='ignore')
                .str.decode('utf-8')
                .str.replace('\n', ' ', regex=False)
                .str.replace('.', '', regex=False)
                .str.strip()
                .str.upper()
        )
    tipo_cambio = datos_tipo_cambio_()
    tipo_cambio = tipo_cambio[["FECHA","PrecioVenta"]]
    tipo_cambio = tipo_cambio.rename(columns={"PrecioVenta":"TIPO_CAMBIO"})
    transport_df = pd.merge(transport_df,tipo_cambio,on=["FECHA"],how="left")
    transport_df["TARIFA $"] = transport_df["TARIFA"]/transport_df["TIPO_CAMBIO"]
    transport_df["FUNDO"] = transport_df["FUNDO"].str.strip()
    transport_df["FUNDO"] = transport_df["FUNDO"].replace({
        "QBERRIES":"QBERRIES II MAGICA"
    })
    transport_df["CAMPAÑA"] = "CAMPAÑA 2026"
    transport_df["VARIEDAD"] = "MAGICA"
    transport_df = transport_df[["SEMANA","FECHA","FUNDO","TARIFA $","CAMPAÑA","VARIEDAD"]]
    #
    
    df = pd.merge(df,transport_df,on=["FECHA","FUNDO","SEMANA","CAMPAÑA","VARIEDAD"],how="outer")
    for col_num in col_numericos:
            df[col_num] = df[col_num].fillna(0)
            #actividad_df[col_num] = actividad_df[col_num].astype(str)
            df[col_num] = df[col_num].astype(float)    
            

    
    transp_df = transp_df[transp_df["FECHA"]>='2026-06-01']
    #transp_df = transp_df[['SEMANA', 'FECHA',  'PUNTO DE PARTIDA', 'UBIC. FUNDOS', 'COSTO', ]]
    cols_pivot = [
    'GAP (S/)', 
    'SAN JOSE II (S/)', 
    'SAN JOSE (S/)',
    'CANYON (S/)', 
    'SAN PEDRO (S/)', 
    'BIG BERRIES (S/)', 
    'GOLDEN BERRIES (S/)', 
    'QBERRIES (S/)', 
    'QBERRIES 2 (S/)', 
    'QBERRIES 3 (S/)', 
    'TARA (S/)', 
    ]
    transp_df = transp_df[['SEMANA', 'FECHA']+cols_pivot]


    # --- Despivotear (wide -> long) ---
    transp_df = transp_df.melt(
        id_vars=['SEMANA', 'FECHA'],
        value_vars=cols_pivot,
        var_name="FUNDO",
        value_name="MONTO (S/)",
    )

    transp_df["FUNDO"] = transp_df["FUNDO"].replace(
        {
        'GAP (S/)':'GAP',
        'SAN JOSE II (S/)':"SAN JOSE II",
        'SAN JOSE (S/)':"SAN JOSE",
        'CANYON (S/)':"CANYON MAGIC",
        'SAN PEDRO (S/)':"SAN PEDRO",
        'BIG BERRIES (S/)':"LA COLINA", 
        'GOLDEN BERRIES (S/)':"LA COLINA",
        'QBERRIES (S/)':"QBERRIES I",
        'QBERRIES 2 (S/)':"QBERRIES II MAGICA",
        'QBERRIES 3 (S/)':"QBERRIES II SEKOYA", 
        'TARA (S/)' :"LAS BRISAS"
        }
    )
    transp_df = transp_df[transp_df["MONTO (S/)"]>0]
    transp_df["FECHA"] = pd.to_datetime(transp_df["FECHA"], errors="coerce")
    transp_df = pd.merge(transp_df,tipo_cambio,on=["FECHA"],how="left")
    transp_df["MONTO($)"] = transp_df["MONTO (S/)"]/transp_df["TIPO_CAMBIO"]
    return df,transp_df

def load_costos_cosecha_excel():
    print(f"📤 Subiendo archivos 'COSTOS COSECHA' a OneDrive...")
    dff,transporte_df = costos_cosecha_2026()
    resultado_1 = subir_archivo_con_reintento(
        access_token=get_access_token(),
        dataframe=dff,
        nombre_archivo="COSTOS_COSECHA.parquet",
        drive_id="b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        folder_id="01KM43WT4FS6JNXKKHRNCJZMAXLX56IOEQ",
        type_file="parquet"
    )
    if resultado_1:
        print(f"✅ Proceso 1 completado exitosamente")
    
    resultado_2 = subir_archivo_con_reintento(
        access_token=get_access_token(),
        dataframe=transporte_df,
        nombre_archivo="COSTOS_TRANSPORTE.parquet",
        drive_id="b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s",
        folder_id="01KM43WT4FS6JNXKKHRNCJZMAXLX56IOEQ",
        type_file="parquet"
    )
    if resultado_2:
        print(f"✅ Proceso 2 completado exitosamente")
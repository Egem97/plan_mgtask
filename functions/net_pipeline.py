"""
Pipeline de extracción de datos desde NetSuite vía SuiteQL.

Ejecuta la query definida en `ordenes.sql` (raíz del proyecto) contra el
servicio REST SuiteQL de NetSuite usando autenticación OAuth 1.0 TBA
(Token-Based Authentication, HMAC-SHA256) y guarda el resultado en `data/`.

La firma OAuth 1.0 se implementa con la librería estándar (hmac/hashlib),
por lo que no requiere dependencias adicionales como requests_oauthlib.

Uso:
    python -m functions.net_pipeline            # corre el scheduler (cada 30 min)
    python -m functions.net_pipeline --once     # ejecuta una sola vez y termina
"""

import os
import sys
import time
import hmac
import base64
import hashlib
import logging
import secrets
from datetime import datetime
from urllib.parse import quote

import requests
import pandas as pd

from utils.config import load_config
from utils.get_token import get_access_token
from utils.get_api import subir_archivo_con_reintento

# Directorios base del proyecto
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SQL_PATH = os.path.join(BASE_DIR, "ordenes.sql")
DATA_DIR = os.path.join(BASE_DIR, "data")

# Nombre del archivo de salida local (en data/)
OUTPUT_NAME = "ordenes_netsuite"

# Cada cuánto se actualiza la extracción
INTERVALO_MINUTOS = 30

# === Destino en OneDrive ===
ONEDRIVE_DRIVE_ID = "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s"
ONEDRIVE_FOLDER_ID = "01KM43WTYSYYWQKAFUXVDJTD4AT3KMQSXK"
ONEDRIVE_FILENAME = "OC PACKING NETSUITE.parquet"

# Tamaño de página de SuiteQL (máximo permitido por NetSuite)
PAGE_SIZE = 1000

# === Esquema de salida ===
# El ORDEN de esta lista define el orden de las columnas del DataFrame y
# refleja exactamente el orden de los alias de la query `ordenes.sql`.
# El segundo valor es el tipo de dato pandas (nullable) a forzar.
# Si modificas ordenes.sql, actualiza también esta lista.
COLUMN_SCHEMA = [
    ("id_transaccion",         "Int64"),
    ("fecha_transaccion",      "datetime64[ns]"),
    ("voucher_contable",       "string"),
    ("empresa",                "string"),
    ("razon_social",           "string"),
    ("glosa",                  "string"),
    ("moneda",                 "string"),
    ("soles_cargo",            "float64"),
    ("soles_abono",            "float64"),
    ("soles_saldo",            "float64"),
    ("dolares_cargo",          "float64"),
    ("dolares_abono",          "float64"),
    ("dolares_saldo",          "float64"),
    ("estado_orden",           "string"),
    ("facturas_vinculadas",    "string"),
    ("estado_pago",            "string"),
    ("departamento",           "string"),
    ("clase",                  "string"),
    ("almacen",                "string"),
    ("cod_item",               "Int64"),
    ("item",                   "string"),
    ("cantidad",               "float64"),
    ("unidad_medida",          "string"),
    ("proyecto",               "string"),
    ("parcela",                "string"),
    ("linea_negocio",          "string"),
    ("actividad",              "string"),
    ("partida_presupuestaria", "string"),
    ("macropartida",           "string"),
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("net_pipeline")


def _get_netsuite_config():
    """Lee las credenciales de NetSuite desde config.yaml."""
    config = load_config()
    if not config:
        raise RuntimeError("No se pudo cargar config.yaml")

    ns = config.get("netsuite") or {}
    requeridos = [
        "ACCOUNT_ID",
        "NETSUITE_CONSUMER_KEY",
        "NETSUITE_CONSUMER_SECRET",
        "NETSUITE_TOKEN_ID",
        "NETSUITE_TOKEN_SECRET",
    ]
    faltantes = [k for k in requeridos if not ns.get(k)]
    if faltantes:
        raise ValueError(
            f"Faltan credenciales de NetSuite en config.yaml: {', '.join(faltantes)}"
        )
    return ns


def _suiteql_base_url(ns):
    """Construye la URL base del endpoint SuiteQL.

    El host usa el account id en minúsculas y con guiones en lugar de
    guiones bajos (relevante para cuentas sandbox como 1234567_SB1).
    """
    account_host = str(ns["ACCOUNT_ID"]).lower().replace("_", "-")
    return (
        f"https://{account_host}.suitetalk.api.netsuite.com"
        "/services/rest/query/v1/suiteql"
    )


def _percent_encode(value):
    """Codificación percent según RFC 3986 (requerida por OAuth 1.0)."""
    return quote(str(value), safe="~")


def _build_oauth_header(ns, method, base_url, query_params):
    """Genera el header Authorization OAuth 1.0 (HMAC-SHA256) para NetSuite.

    Los parámetros de query (limit, offset) se incluyen en la firma, como
    exige la especificación OAuth 1.0.
    """
    realm = str(ns.get("REALM_ID") or ns["ACCOUNT_ID"])

    oauth_params = {
        "oauth_consumer_key": ns["NETSUITE_CONSUMER_KEY"],
        "oauth_token": ns["NETSUITE_TOKEN_ID"],
        "oauth_signature_method": "HMAC-SHA256",
        "oauth_timestamp": str(int(time.time())),
        "oauth_nonce": secrets.token_hex(16),
        "oauth_version": "1.0",
    }

    # Parámetros a firmar = oauth_* + parámetros de query
    todos = {**oauth_params, **{k: str(v) for k, v in query_params.items()}}
    param_string = "&".join(
        f"{_percent_encode(k)}={_percent_encode(todos[k])}"
        for k in sorted(todos)
    )

    base_string = "&".join([
        method.upper(),
        _percent_encode(base_url),
        _percent_encode(param_string),
    ])

    signing_key = (
        f"{_percent_encode(ns['NETSUITE_CONSUMER_SECRET'])}"
        f"&{_percent_encode(ns['NETSUITE_TOKEN_SECRET'])}"
    )

    signature = base64.b64encode(
        hmac.new(
            signing_key.encode("utf-8"),
            base_string.encode("utf-8"),
            hashlib.sha256,
        ).digest()
    ).decode("utf-8")

    oauth_params["oauth_signature"] = signature

    header_params = {"realm": realm, **oauth_params}
    header = "OAuth " + ", ".join(
        f'{_percent_encode(k)}="{_percent_encode(v)}"'
        for k, v in header_params.items()
    )
    return header


def leer_query(path=SQL_PATH):
    """Lee la sentencia SQL desde el archivo .sql."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"No se encontró el archivo de query: {path}")
    with open(path, "r", encoding="utf-8") as f:
        query = f.read().strip()
    if not query:
        raise ValueError(f"El archivo de query está vacío: {path}")
    return query


def ejecutar_suiteql(query, ns=None):
    """Ejecuta una query SuiteQL en NetSuite manejando la paginación.

    Devuelve la lista completa de filas (lista de dicts).
    """
    if ns is None:
        ns = _get_netsuite_config()

    base_url = _suiteql_base_url(ns)

    filas = []
    offset = 0
    while True:
        query_params = {"limit": PAGE_SIZE, "offset": offset}
        auth_header = _build_oauth_header(ns, "POST", base_url, query_params)
        headers = {
            "Authorization": auth_header,
            "Content-Type": "application/json",
            "Accept": "application/json",
            # Requerido por SuiteQL para resultados transitorios
            "Prefer": "transient",
        }

        response = requests.post(
            base_url,
            params=query_params,
            headers=headers,
            json={"q": query},
            timeout=120,
        )

        if response.status_code != 200:
            raise RuntimeError(
                f"Error SuiteQL ({response.status_code}): {response.text}"
            )

        data = response.json()
        items = data.get("items", [])
        filas.extend(items)
        logger.info(
            "Página offset=%s -> %s filas (acumuladas: %s)",
            offset,
            len(items),
            len(filas),
        )

        if not data.get("hasMore"):
            break
        offset += PAGE_SIZE

    return filas


def _apply_schema(df):
    """Ordena y tipa el DataFrame según COLUMN_SCHEMA.

    - Crea como vacías las columnas del esquema que NetSuite omitió
      (SuiteQL no devuelve la clave cuando el valor es nulo en todas las filas).
    - Fuerza el tipo de dato de cada columna.
    - Reordena dejando primero las columnas del esquema (en su orden) y al
      final cualquier columna extra inesperada que haya devuelto la query.
    """
    for col, dtype in COLUMN_SCHEMA:
        if col not in df.columns:
            df[col] = pd.NA

        try:
            if dtype == "datetime64[ns]":
                df[col] = pd.to_datetime(df[col], errors="coerce")
            elif dtype == "Int64":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif dtype == "float64":
                # .astype fuerza float aunque todos los valores sean enteros
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
            else:  # string
                df[col] = df[col].astype("string")
        except Exception as exc:  # no abortar todo por una columna problemática
            logger.warning("No se pudo convertir '%s' a %s: %s", col, dtype, exc)

    ordenadas = [c for c, _ in COLUMN_SCHEMA]
    extras = [c for c in df.columns if c not in ordenadas]
    if extras:
        logger.warning("Columnas no contempladas en el esquema: %s", extras)
    return df[ordenadas + extras]


def _to_dataframe(filas):
    """Convierte las filas a un DataFrame de pandas, ordenado y tipado."""
    # SuiteQL agrega un campo "links" por fila que no aporta datos
    limpias = [{k: v for k, v in fila.items() if k != "links"} for fila in filas]
    df = pd.DataFrame(limpias)
    return _apply_schema(df)


def guardar_resultado(df, nombre=OUTPUT_NAME):
    """Guarda el DataFrame en data/ como parquet y csv."""
    os.makedirs(DATA_DIR, exist_ok=True)
    ruta_parquet = os.path.join(DATA_DIR, f"{nombre}.parquet")
    ruta_csv = os.path.join(DATA_DIR, f"{nombre}.csv")

    df.to_parquet(ruta_parquet, index=False)
    df.to_csv(ruta_csv, index=False, encoding="utf-8-sig")

    logger.info("Resultado guardado en: %s", ruta_parquet)
    logger.info("Resultado guardado en: %s", ruta_csv)
    return ruta_parquet


def ejecutar_pipeline():
    """Ejecuta el pipeline completo: lee la query, extrae y guarda."""
    inicio = datetime.now()
    logger.info("=" * 60)
    logger.info("Iniciando extracción SuiteQL de NetSuite")

    try:
        ns = _get_netsuite_config()
        query = leer_query()
        filas = ejecutar_suiteql(query, ns)

        if not filas:
            logger.warning("La query no devolvió filas. No se guardó nada.")
            return None

        df = _to_dataframe(filas)
        logger.info("Filas obtenidas: %s | Columnas: %s", len(df), len(df.columns))
        ruta = guardar_resultado(df)

        duracion = (datetime.now() - inicio).total_seconds()
        logger.info("Pipeline completado en %.1f s", duracion)
        return ruta

    except Exception as exc:
        logger.exception("Error en el pipeline de NetSuite: %s", exc)
        return None


def pipeline_netsuite_ordenes():
    """Extrae las órdenes desde NetSuite y las sube a OneDrive.

    Esta es la función pensada para el scheduler (schedule.py). Devuelve
    True si la subida fue exitosa, False en caso de error.
    """
    inicio = datetime.now()
    logger.info("=" * 60)
    logger.info("Iniciando extracción SuiteQL de NetSuite -> OneDrive")

    try:
        ns = _get_netsuite_config()
        query = leer_query()
        filas = ejecutar_suiteql(query, ns)

        if not filas:
            logger.warning("La query no devolvió filas. No se subió nada.")
            return False

        df = _to_dataframe(filas)
        logger.info("Filas obtenidas: %s | Columnas: %s", len(df), len(df.columns))

        resultado = subir_archivo_con_reintento(
            access_token=get_access_token(),
            dataframe=df,
            nombre_archivo=ONEDRIVE_FILENAME,
            drive_id=ONEDRIVE_DRIVE_ID,
            folder_id=ONEDRIVE_FOLDER_ID,
            type_file="parquet",
        )

        duracion = (datetime.now() - inicio).total_seconds()
        if resultado:
            logger.info(
                "✅ '%s' subido a OneDrive en %.1f s", ONEDRIVE_FILENAME, duracion
            )
            return True

        logger.error("❌ Error al subir '%s' a OneDrive", ONEDRIVE_FILENAME)
        return False

    except Exception as exc:
        logger.exception("Error en el pipeline NetSuite -> OneDrive: %s", exc)
        return False


def iniciar_scheduler(intervalo_minutos=INTERVALO_MINUTOS, ejecutar_inicial=True):
    """Programa la ejecución del pipeline cada N minutos (APScheduler)."""
    from apscheduler.schedulers.blocking import BlockingScheduler

    logger.info(
        "Scheduler iniciado: extracción cada %s minutos", intervalo_minutos
    )

    if ejecutar_inicial:
        pipeline_netsuite_ordenes()

    scheduler = BlockingScheduler(timezone="America/Lima")
    scheduler.add_job(
        pipeline_netsuite_ordenes,
        trigger="interval",
        minutes=intervalo_minutos,
        id="netsuite_ordenes",
        max_instances=1,
        coalesce=True,
    )

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler detenido.")

"""
if __name__ == "__main__":
    # Evita UnicodeEncodeError al imprimir emojis (✅/❌) en consolas cp1252 (Windows)
    for _stream in (sys.stdout, sys.stderr):
        try:
            _stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass

    if "--local" in sys.argv:
        # Solo extrae y guarda en data/ (sin subir a OneDrive)
        ejecutar_pipeline()
    elif "--once" in sys.argv:
        # Extrae y sube a OneDrive una sola vez
        pipeline_netsuite_ordenes()
    else:
        # Scheduler: sube a OneDrive cada 30 min
        iniciar_scheduler()
"""
"""
Pipeline CDC del Mayor Analítico (NetSuite SuiteQL) — conglomerado multi-subsidiaria.

Subsidiarias: 2, 3, 5, 6, 9, 10, 12, 13, 14, 15, 19
Grano (clave única) por fila: (id_transaccion, id_linea) = (t.id, tl.id).

Estrategia
----------
1. Carga completa (`full_load`): trae TODO el mayor analítico y lo guarda como
   parquet. Es además la "red de seguridad" semanal (reconcilia todo desde cero).

2. Incremental (`incremental`, cada 30 min) — CDC:
   - NUEVOS / MODIFICADOS: filtra `lastmodifieddate >= hoy - OVERLAP` y hace
     *delete-insert por id_transaccion* (reemplaza TODAS las líneas de cada
     transacción cambiada → cubre líneas agregadas y quitadas dentro del asiento).
   - ELIMINADOS: como la tabla `deletedrecord` no está expuesta en SuiteQL en
     esta cuenta, se reconcilia el set de ids de transacción: se traen los ids
     vigentes en NetSuite y se eliminan del parquet los que ya no existen
     (transacción borrada o que dejó de calificar el filtro).

Paginación
----------
SuiteQL pagina por bloques de 1000. Para no duplicar/saltar filas cuando los
datos cambian durante la extracción se usa **keyset pagination** por la clave
ordenada (id_transaccion, id_linea), en lugar de offset.

Uso:
    python -m functions.mayor_analitico_pipeline --full   # carga completa inicial
    python -m functions.mayor_analitico_pipeline          # incremental (CDC)
"""

import os
import re
import sys
import json
import logging
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
import pandas as pd

from utils.get_token import get_access_token
from utils.get_api import subir_archivo_con_reintento
from functions.net_pipeline import (
    _get_netsuite_config,
    _suiteql_base_url,
    _build_oauth_header,
)

# === Rutas ===
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SQL_PATH = os.path.join(BASE_DIR, "mayor_analitico.sql")
DATA_DIR = os.path.join(BASE_DIR, "data")
PARQUET_PATH = os.path.join(DATA_DIR, "mayor_analitico.parquet")
STATE_PATH = os.path.join(DATA_DIR, "mayor_analitico_state.json")

# === Parámetros CDC ===
SUBSIDIARIES = [2, 3, 5, 6, 9, 10, 12, 13, 14, 15, 19]
OVERLAP_DAYS = 2            # solapamiento de seguridad del incremental
PAGE_SIZE = 1000           # máximo de SuiteQL
TZ = ZoneInfo("America/Lima")

# === Destino OneDrive ===
ONEDRIVE_DRIVE_ID = "b!7vn8i7N-DE-ulN73jRlvqAu5qgW8g95Cn8TCfsKkQKdsTPblFTr2TIQQJcSPyz9s"
ONEDRIVE_FOLDER_ID = "01KM43WT32ZAVHOVX6NJBI6CYALR5ZC6GM"
ONEDRIVE_FILENAME = "MAYOR ANALITICO NETSUITE.parquet"

# === Esquema de salida (orden EXACTO de mayor_analitico.sql + tipo) ===
COLUMN_SCHEMA = [
    ("cuenta",                 "string"),
    ("nombre_cta_contable",    "string"),
    ("nombre_cta_contable2",   "string"),
    ("tipo_cuenta_netsuite",   "string"),
    ("tipo_cuenta_es",         "string"),
    ("id_transaccion",         "Int64"),
    ("id_linea",               "Int64"),
    ("fecha_modificacion",     "datetime64[ns]"),
    ("fecha_transaccion",      "datetime64[ns]"),
    ("voucher_contable",       "string"),
    ("tipo_transaccion",       "string"),
    ("tipo_transaccion_es",    "string"),
    ("glosa",                  "string"),
    ("soles_cargo",            "float64"),
    ("soles_abono",            "float64"),
    ("soles_saldo",            "float64"),
    ("dolares_cargo",          "float64"),
    ("dolares_abono",          "float64"),
    ("dolares_saldo",          "float64"),
    ("moneda",                 "string"),
    ("empresa",                "string"),
    ("id_subsidiaria",         "Int64"),
    ("razon_social",           "string"),
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
logger = logging.getLogger("mayor_analitico")

# Cache de credenciales / URL
_NS = None
_BASE_URL = None

# Serializa full_load e incremental para que nunca escriban el parquet a la vez
# (p. ej. cuando el full semanal coincide con un incremental de los :00).
# RLock para permitir el fallback incremental() -> full_load() sin deadlock.
_LOCK = threading.RLock()


def _serialized(fn):
    """Decorador: serializa la ejecución de la función con _LOCK."""
    def wrapper(*args, **kwargs):
        with _LOCK:
            return fn(*args, **kwargs)
    wrapper.__name__ = fn.__name__
    wrapper.__doc__ = fn.__doc__
    return wrapper


def _ns():
    global _NS, _BASE_URL
    if _NS is None:
        _NS = _get_netsuite_config()
        _BASE_URL = _suiteql_base_url(_NS)
    return _NS, _BASE_URL


# --------------------------------------------------------------------------- #
# Acceso a SuiteQL
# --------------------------------------------------------------------------- #
def _suiteql_page(query, limit=PAGE_SIZE, offset=0):
    """Una sola petición SuiteQL. Devuelve el JSON parseado."""
    ns, base_url = _ns()
    params = {"limit": limit, "offset": offset}
    headers = {
        "Authorization": _build_oauth_header(ns, "POST", base_url, params),
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Prefer": "transient",
    }
    resp = requests.post(base_url, params=params, headers=headers,
                         json={"q": query}, timeout=180)
    if resp.status_code != 200:
        raise RuntimeError(f"Error SuiteQL ({resp.status_code}): {resp.text[:500]}")
    return resp.json()


def _inner_query():
    """Lee mayor_analitico.sql y lo deja listo para envolver como subconsulta.

    Quita el ORDER BY final y el punto y coma para poder usarlo como
    subconsulta con keyset pagination.
    """
    with open(SQL_PATH, "r", encoding="utf-8") as f:
        sql = f.read()
    sql = sql.strip().rstrip(";").strip()
    # Elimina el ORDER BY de nivel superior (no hay subconsultas con ORDER BY).
    sql = re.sub(r"(?is)\border\s+by\b[^)]*$", "", sql).strip()
    return sql


def _fetch_keyset(extra_predicate=""):
    """Pagina por keyset (id_transaccion, id_linea) toda la consulta envuelta.

    `extra_predicate` se añade al WHERE del wrapper (ej. filtro CDC por fecha).
    Devuelve lista de dicts (filas), sin metadatos.
    """
    inner = _inner_query()
    filas = []
    last_id, last_line = -1, -1
    pagina = 0
    while True:
        cursor = (f"(q.id_transaccion > {last_id} OR "
                  f"(q.id_transaccion = {last_id} AND q.id_linea > {last_line}))")
        query = (
            f"SELECT * FROM ({inner}) q "
            f"WHERE {cursor} {extra_predicate} "
            f"ORDER BY q.id_transaccion, q.id_linea"
        )
        data = _suiteql_page(query, limit=PAGE_SIZE)
        items = data.get("items", [])
        if not items:
            break
        for it in items:
            it.pop("links", None)
        filas.extend(items)
        last = items[-1]
        last_id = int(last["id_transaccion"])
        last_line = int(last["id_linea"])
        pagina += 1
        if pagina % 10 == 0:
            logger.info("  ... %s filas acumuladas (cursor %s,%s)",
                        len(filas), last_id, last_line)
        if not data.get("hasMore"):
            break
    return filas


def _fetch_current_txn_ids():
    """Trae el set de ids de transacción vigentes en NetSuite bajo el filtro.

    Liviano (solo el id), con keyset por t.id. Sirve para detectar borrados.
    """
    subs = ", ".join(str(s) for s in SUBSIDIARIES)
    ids = set()
    last_id = -1
    while True:
        query = f"""
            SELECT t.id AS id
            FROM transactionLine tl
              INNER JOIN transaction t ON t.id = tl.transaction
              INNER JOIN transactionAccountingLine tal
                 ON tal.transaction = tl.transaction
                AND tal.transactionline = tl.id
                AND tal.accountingbook = 1
              INNER JOIN account a ON a.id = tal.account
            WHERE t.posting = 'T'
              AND tl.subsidiary IN ({subs})
              AND t.id > {last_id}
            GROUP BY t.id
            ORDER BY t.id
        """
        data = _suiteql_page(query, limit=PAGE_SIZE)
        items = data.get("items", [])
        if not items:
            break
        for it in items:
            ids.add(int(it["id"]))
        last_id = int(items[-1]["id"])
        if not data.get("hasMore"):
            break
    return ids


# --------------------------------------------------------------------------- #
# Transformación
# --------------------------------------------------------------------------- #
def _apply_schema(df):
    """Ordena las columnas según COLUMN_SCHEMA y fuerza los tipos."""
    for col, dtype in COLUMN_SCHEMA:
        if col not in df.columns:
            df[col] = pd.NA
        try:
            if dtype == "datetime64[ns]":
                df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
            elif dtype == "Int64":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif dtype == "float64":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
            else:
                df[col] = df[col].astype("string")
        except Exception as exc:
            logger.warning("No se pudo convertir '%s' a %s: %s", col, dtype, exc)

    ordenadas = [c for c, _ in COLUMN_SCHEMA]
    extras = [c for c in df.columns if c not in ordenadas]
    if extras:
        logger.warning("Columnas fuera de esquema: %s", extras)
    return df[ordenadas + extras]


def _to_dataframe(filas):
    """Lista de dicts -> DataFrame ordenado y tipado."""
    df = pd.DataFrame(filas)
    df = _apply_schema(df)
    return df


# --------------------------------------------------------------------------- #
# Estado / persistencia
# --------------------------------------------------------------------------- #
def _load_state():
    if not os.path.exists(STATE_PATH):
        return None
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("No se pudo leer el state (%s); se hará carga completa.", exc)
        return None


def _save_state(state):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def _guardar_parquet(df):
    os.makedirs(DATA_DIR, exist_ok=True)
    df.to_parquet(PARQUET_PATH, index=False)
    logger.info("Parquet guardado: %s (%s filas)", PARQUET_PATH, len(df))


def _subir_onedrive(df):
    """Sube el parquet a OneDrive. Devuelve True/False."""
    resultado = subir_archivo_con_reintento(
        access_token=get_access_token(),
        dataframe=df,
        nombre_archivo=ONEDRIVE_FILENAME,
        drive_id=ONEDRIVE_DRIVE_ID,
        folder_id=ONEDRIVE_FOLDER_ID,
        type_file="parquet",
    )
    if resultado:
        logger.info("✅ '%s' subido a OneDrive", ONEDRIVE_FILENAME)
    else:
        logger.error("❌ No se pudo subir '%s' a OneDrive", ONEDRIVE_FILENAME)
    return resultado


# --------------------------------------------------------------------------- #
# Pipelines
# --------------------------------------------------------------------------- #
@_serialized
def full_load(upload=True):
    """Carga completa del mayor analítico (también red de seguridad semanal)."""
    inicio = datetime.now(TZ)
    logger.info("=" * 70)
    logger.info("MAYOR ANALÍTICO — carga COMPLETA")

    try:
        filas = _fetch_keyset()
        df = _to_dataframe(filas)
        logger.info("Filas: %s | Columnas: %s | Txns: %s",
                    len(df), len(df.columns), df["id_transaccion"].nunique())

        _guardar_parquet(df)
        _save_state({
            "last_run": inicio.isoformat(),
            "last_full_load": inicio.isoformat(),
            "rows": int(len(df)),
            "mode": "full",
        })
        if upload:
            _subir_onedrive(df)

        logger.info("Carga completa OK en %.1f s",
                    (datetime.now(TZ) - inicio).total_seconds())
        return True
    except Exception as exc:
        logger.exception("Error en carga completa: %s", exc)
        return False


@_serialized
def incremental(upload=True):
    """Sincroniza cambios (inserts/updates/deletes) con CDC."""
    inicio = datetime.now(TZ)
    logger.info("=" * 70)
    logger.info("MAYOR ANALÍTICO — incremental (CDC)")

    state = _load_state()
    if state is None or not os.path.exists(PARQUET_PATH):
        logger.info("Sin estado/parquet previo → se ejecuta carga completa.")
        return full_load(upload=upload)

    try:
        # Watermark: desde la última corrida menos el solapamiento.
        last_run = datetime.fromisoformat(state["last_run"])
        if last_run.tzinfo is None:
            last_run = last_run.replace(tzinfo=TZ)
        low = (min(last_run, inicio) - timedelta(days=OVERLAP_DAYS))
        low_str = low.strftime("%Y-%m-%d 00:00:00")
        logger.info("Ventana CDC: cambios con lastmodifieddate >= %s", low_str)

        # 1) NUEVOS / MODIFICADOS
        extra = (f"AND q.fecha_modificacion >= "
                 f"TO_TIMESTAMP('{low_str}', 'YYYY-MM-DD HH24:MI:SS')")
        fresh_df = _to_dataframe(_fetch_keyset(extra_predicate=extra))
        fresh_ids = set(int(x) for x in fresh_df["id_transaccion"].dropna().unique())
        logger.info("Cambios detectados: %s filas en %s transacciones",
                    len(fresh_df), len(fresh_ids))

        # 2) BORRADOS (reconciliación de ids de transacción)
        current_ids = _fetch_current_txn_ids()
        logger.info("Transacciones vigentes en NetSuite: %s", len(current_ids))

        # 3) Aplicar sobre el parquet existente
        store = pd.read_parquet(PARQUET_PATH)
        store_ids = set(int(x) for x in store["id_transaccion"].dropna().unique())
        deleted_ids = store_ids - current_ids
        logger.info("Transacciones eliminadas/no vigentes: %s", len(deleted_ids))

        a_remover = fresh_ids | deleted_ids
        store = store[~store["id_transaccion"].isin(a_remover)]
        out = pd.concat([store, fresh_df], ignore_index=True)
        out = _apply_schema(out)
        out = out.sort_values(["id_transaccion", "id_linea"]).reset_index(drop=True)

        _guardar_parquet(out)
        _save_state({
            "last_run": inicio.isoformat(),
            "last_full_load": state.get("last_full_load"),
            "rows": int(len(out)),
            "mode": "incremental",
            "ultimo_incremental": {
                "filas_upsert": int(len(fresh_df)),
                "txns_cambiadas": len(fresh_ids),
                "txns_eliminadas": len(deleted_ids),
            },
        })
        if upload:
            _subir_onedrive(out)

        logger.info(
            "Incremental OK en %.1f s | total filas: %s (+%s upsert / -%s borradas)",
            (datetime.now(TZ) - inicio).total_seconds(),
            len(out), len(fresh_df), len(deleted_ids),
        )
        return True
    except Exception as exc:
        logger.exception("Error en incremental: %s", exc)
        return False


if __name__ == "__main__":
    for _stream in (sys.stdout, sys.stderr):
        try:
            _stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass

    if "--full" in sys.argv:
        full_load()
    else:
        incremental()

import requests
import threading
import time
from typing import Optional
from utils.config import load_config



# Cargar configuración al inicializar el módulo
config = load_config()

# Se renueva el token este margen ANTES del expires_in real, para que no caduque
# a mitad de una operación larga (subida de parquet, lectura de Excel remoto).
_TOKEN_REFRESH_MARGIN = 300  # segundos


class _TokenCache:
    """Cachea un access_token hasta que expira.

    Thread-safe: el lock se mantiene durante el fetch, así N hilos concurrentes
    piden UN solo token en vez de N (los que llegan después esperan y reutilizan
    el que trajo el primero). Los fallos no se cachean.
    """

    def __init__(self, fetch):
        self._fetch = fetch
        self._lock = threading.Lock()
        self._token = None
        self._previous_token = None
        self._expires_at = 0.0

    def _fetch_locked(self) -> Optional[str]:
        """Pide un token nuevo y lo cachea. Requiere tener el lock tomado."""
        token, expires_in = self._fetch()
        if not token:
            return None

        self._token = token
        self._expires_at = time.monotonic() + max(expires_in - _TOKEN_REFRESH_MARGIN, 0)
        return token

    def get(self) -> Optional[str]:
        with self._lock:
            if self._token and time.monotonic() < self._expires_at:
                return self._token
            return self._fetch_locked()

    def refresh(self, stale_token: str) -> Optional[str]:
        """Renueva el token si `stale_token` es el que emitió este cache.

        Devuelve el token nuevo, o None si `stale_token` no salió de este cache
        (así el llamador puede probar con otro tenant).
        """
        with self._lock:
            if stale_token and stale_token == self._token:
                self._previous_token = self._token
                return self._fetch_locked()

            # Otro hilo ya lo renovó mientras este esperaba el lock (típico con
            # varios uploads en paralelo caducando a la vez): el token vigente
            # ya es el bueno, no hace falta pedir otro.
            if stale_token and stale_token == self._previous_token:
                return self._token

            return None

    def invalidate(self):
        """Fuerza pedir un token nuevo en el próximo get() (p. ej. tras un 401)."""
        with self._lock:
            self._previous_token = self._token
            self._token = None
            self._expires_at = 0.0


def _request_token(section: str):
    """Pide un token nuevo a Microsoft. Devuelve (token, expires_in) o (None, 0)."""
    if not config:
        print("Error: No se pudo cargar la configuración")
        return None, 0

    AUTHORITY = f"https://login.microsoftonline.com/{config[section]['tenant_id']}/oauth2/v2.0/token"
    try:
        response = requests.post(AUTHORITY, data={
            "grant_type": "client_credentials",
            "client_id": config[section]['client_id'],
            "client_secret": config[section]['client_secret'],
            "scope": "https://graph.microsoft.com/.default"
        })

        if response.status_code == 200:
            token_response = response.json()
            access_token = token_response.get("access_token")

            if access_token:
                print("Token de acceso obtenido exitosamente")
                # expires_in viene en segundos (Azure devuelve ~3600).
                return access_token, int(token_response.get("expires_in", 3600))
            else:
                print("Error: No se pudo obtener el token de acceso")
                return None, 0
        else:
            print(f"Error HTTP {response.status_code}: {response.text}")
            return None, 0

    except Exception as e:
        print(f"Error al obtener el token: {e}")
        return None, 0


_token_cache = _TokenCache(lambda: _request_token('microsoft_graph'))
_token_cache_packing = _TokenCache(lambda: _request_token('microsoft_graph_packing'))


def get_access_token() -> Optional[str]:
    """
    Obtiene el token de acceso para Microsoft Graph API.
    Cacheado hasta que expira y compartido entre hilos.
    """
    return _token_cache.get()


def refresh_access_token(stale_token: str) -> Optional[str]:
    """Renueva un token que Graph rechazó con 401 y devuelve el nuevo.

    El llamador solo tiene el string del token (no sabe de qué tenant salió), así
    que se identifica por valor contra cada cache. Devuelve None si no salió de
    ninguno o si no se pudo renovar; en ese caso el 401 no era por token vencido.
    """
    for cache in (_token_cache, _token_cache_packing):
        nuevo = cache.refresh(stale_token)
        if nuevo:
            return nuevo
    return None

def get_config_value(section: str, key: str = None):
    """
    Obtiene un valor específico de la configuración
    
    Args:
        section: Sección del config (ej: 'microsoft_graph', 'onedrive', etc.)
        key: Clave específica dentro de la sección (opcional)
    
    Returns:
        El valor solicitado o None si no existe
    """
    if not config:
        print("Error: No se pudo cargar la configuración")
        return None
    
    if section not in config:
        print(f"Error: La sección '{section}' no existe en la configuración")
        return None
    
    if key is None:
        return config[section]
    
    if key not in config[section]:
        print(f"Error: La clave '{key}' no existe en la sección '{section}'")
        return None
    
    return config[section][key]

def print_config():
    """
    Imprime toda la configuración de manera organizada
    """
    if not config:
        print("Error: No se pudo cargar la configuración")
        return
    
    print("=== CONFIGURACIÓN CARGADA ===")
    for section, values in config.items():
        print(f"\n[{section.upper()}]")
        if isinstance(values, dict):
            for key, value in values.items():
                # Ocultar valores sensibles
                if 'secret' in key.lower() or 'token' in key.lower():
                    print(f"  {key}: ****")
                else:
                    print(f"  {key}: {value}")
        else:
            print(f"  {values}")

def get_access_token_packing() -> Optional[str]:
    """
    Obtiene el token de acceso para Microsoft Graph API (tenant packing).
    Cacheado hasta que expira y compartido entre hilos.
    """
    return _token_cache_packing.get()
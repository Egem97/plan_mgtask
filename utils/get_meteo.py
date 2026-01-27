import requests
import pandas as pd
from datetime import datetime
from utils.config import load_config
from utils.utils import clean_data_meteorologica

config = load_config()

class InnovaWeatherAPI:
   
    
    def __init__(self):
        self.email = config["innova_weather"]["email"]
        self.password = config["innova_weather"]["password"]
        self.session = requests.Session()
        self.base_url = config["innova_weather"]["base_url"]
        self.logged_in = False
    
    def login(self):
        
        login_url = f"{self.base_url}/login"

        login_data = {
            "email": self.email,
            "password": self.password
        }
        
        try:
            response = self.session.post(login_url, data=login_data)
            
            if response.status_code == 200:
                if self.session.cookies:
                    self.logged_in = True
                    print("✓ Login exitoso")
                    return True
                else:
                    print("✗ Login falló - No se recibieron cookies de sesión")
                    return False
            else:
                print(f"✗ Login falló - Status code: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"✗ Error durante el login: {str(e)}")
            return False
    
    def get_station_data(self, station_id, fecha_inicio, fecha_fin, hora_inicio="0:00", hora_fin="23:59", interval="undefined"):

        if not self.logged_in:
            print("✗ Debe iniciar sesión primero")
            return None
        

        api_url = f"{self.base_url}/Reportes/listarReporteDatosxEstacion"
        
        params = {
            "id": station_id,
            "fecIni": fecha_inicio,
            "horaIni": hora_inicio,
            "fecFin": fecha_fin,
            "horaFin": hora_fin,
            "intrv": interval
        }
        
        try:
            response = self.session.get(api_url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                print(f"✓ Datos obtenidos exitosamente")
                return data
            else:
                print(f"✗ Error al obtener datos - Status code: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"✗ Error al obtener datos: {str(e)}")
            return None
    
    def data_to_dataframe(self, data):

        try:
            if data:
                df = pd.DataFrame(data)
                print(f"✓ DataFrame creado con {len(df)} registros")
                return df
            else:
                print("✗ No hay datos para convertir")
                return None
        except Exception as e:
            print(f"✗ Error al crear DataFrame: {str(e)}")
            return None

    def get_all_stations_data(self, hora_inicio="0:00", hora_fin="23:59", interval="undefined"):

        if not self.logged_in:
            print("✗ Debe iniciar sesión primero")
            return None
        stations_ids = [
            "001D0AE0D1CD",
            "001D0AE0986B",
            "001D0AE09AD9",
            "001D0AE0D39F"
        ]
        api_url = f"{self.base_url}/Reportes/listarReporteDatosxEstacion"
        general_df = pd.DataFrame()    
        for station_id in stations_ids:
            print(station_id)
        
            params = {
                "id": station_id,
                "fecIni": "01/01/2026",
                "horaIni": hora_inicio,
                "fecFin": "31/12/2026",
                "horaFin": hora_fin,
                "intrv": interval
            }
            
            
            response = self.session.get(api_url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                print(f"✓ Datos obtenidos exitosamente")
                df = pd.DataFrame(data)
                df = clean_data_meteorologica(df)
                general_df = pd.concat([general_df, df], ignore_index=True)
            else:
                print(f"✗ Error al obtener datos - Status code: {response.status_code}")
                return None
        
        return general_df
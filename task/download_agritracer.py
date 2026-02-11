from playwright.sync_api import sync_playwright
import time
import pandas as pd
from io import BytesIO
import os
from utils.get_token import load_config
from utils.helpers import create_format_excel_in_memory

config = load_config()

def download_files_c1():
    print("🚀 Ejecutando Playwright en modo headless...")
    with sync_playwright() as p:
        # Lanzar navegador en modo headless (sin ventana visible)
        # Lanzar navegador en modo headless con argumentos adicionales
        browser = p.chromium.launch(
            headless=True,
            args=['--start-maximized']
        )
        
        # Configurar contexto con viewport y user-agent específicos para evitar detección/errores de layout
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        page = context.new_page()
        
        # Configurar el manejo de descargas
        download_url = None
        
        def handle_download(download):
            nonlocal download_url
            download_url = download.url
            print(f"📥 URL de descarga capturada: {download_url}")
        
        # Escuchar eventos de descarga
        page.on("download", handle_download)
        
        # Ir a la página de login
        print("🔐 Navegando a página de login...")
        page.goto("https://alzaperu.agritracer.app/#/auth/login")
        
        # Llenar formulario de login usando XPath
        print("👤 Llenando formulario de login...")
        # Usando XPath para encontrar campos
        page.fill('xpath=//html/body/kt-auth/div/div/div[2]/kt-login/div/div/form/div[1]/mat-form-field/div[1]/div[2]/div/input', config['agritracer']['user'])#
        page.fill('xpath=//html/body/kt-auth/div/div/div[2]/kt-login/div/div/form/div[2]/mat-form-field/div[1]/div[2]/div/input', config['agritracer']['password'])#
        page.click('xpath=//html/body/kt-auth/div/div/div[2]/kt-login/div/div/form/div[3]/button')
        
        # Esperar a que el login se complete y cambie la URL
        print("⏳ Esperando autenticación...")
        page.wait_for_url(lambda url: "auth/login" not in url, timeout=90000)
        #page.wait_for_load_state('networkidle')

        #https://alzaperu.agritracer.app/#/manoobra/reportes/campo/rpt-horas
        #print("📍 Navegando al reporte...")
        # Debug: Imprimir opciones del sidebar para verificar
        print("📍 Inspeccionando sidebar...")
        # Intentar esperar a que el sidebar cargue
        page.wait_for_selector("kt-aside-left ul li", timeout=100000)
        
        # Opción 1: Navegación Directa (Más robusta si la URL es constante)
        # target_url = "https://alzaperu.agritracer.app/#/manoobra/reportes/campo/rpt-horas"
        # print(f"📍 Navegando directamente a: {target_url}")
        # page.goto(target_url)
        # page.wait_for_load_state('networkidle')

        # Opción 2: Navegación por Sidebar (Paso a paso con debug)
        print("📍 Navegando por sidebar (Paso 1)...")
        
        # Paso 1: Primer nivel (Mano de Obra / Operaciones ?)
        # Usamos locator más específico o texto si es posible. Por ahora, hacemos print del texto para confirmar.
        sidebar_item_1 = page.locator("xpath=//html/body/kt-base/div/div/kt-aside-left/div/div/div/ul/li[4]")
        print(f"   Texto del item 1: '{sidebar_item_1.inner_text().strip()}'")
        sidebar_item_1.click()
        time.sleep(1) # Pequeña pausa para animación
        #sidebar_item_1.click()
        
        #time.sleep(1)


        print("📍 Navegando por sidebar (Paso 2)...")
        # Paso 2: Segundo nivel
        # Esperar que el submenu sea visible
        sidebar_item_2 = page.locator("xpath=//html/body/kt-base/div/div/kt-aside-left/div/div/div/ul/li[4]/div")
        # Asegurar que está visible antes de clickear
        if sidebar_item_2.is_visible():
             print(f"   Texto del item 2: '{sidebar_item_2.inner_text().strip()}'")
             sidebar_item_2.click()
        else:
             print("   ⚠️ El item 2 no es visible, intentando forzar espera o selector alternativo...")
             sidebar_item_2.wait_for(state="visible", timeout=5000)
             sidebar_item_2.click()
        time.sleep(1)

        print("📍 Navegando por sidebar (Paso 3)...")
        # Paso 3: Tercer nivel
        #sidebar_item_3 = page.locator("xpath=//html/body/kt-base/div/div/kt-aside-left/div/div/div/ul/li[4]/div")
        #/html/body/kt-base/div/div/kt-aside-left/div/div/div/ul/li[4]/div/ul/li/div/ul/li[2]/a/span
        sidebar_item_3 = page.locator("xpath=//html/body/kt-base/div/div/kt-aside-left/div/div/div/ul/li[4]/div/ul/li/div/ul/li[2]/a/span")
        if sidebar_item_3.is_visible():
            print(f"   Texto del item 3: '{sidebar_item_3.inner_text().strip()}'")
            sidebar_item_3.click()
        else:
             print("   ⚠️ El item 3 no es visible...")
             sidebar_item_3.wait_for(state="visible", timeout=5000)
             sidebar_item_3.click()
        
        # Esperar carga final
        time.sleep(2)
        ###################select date edit
        select_fecha_inicio_doom = "xpath=//html/body/kt-base/div/div/div/div/div/kt-horas-rpt/kt-portlet[1]/div/kt-portlet-body/div/form/div[1]/div/div[3]/div/mat-form-field/div[1]/div[2]/div[1]/mat-date-range-input/div/div[1]/input"
        select_fecha_inicio = page.locator(select_fecha_inicio_doom)
        select_fecha_inicio.press("Control+A")
        select_fecha_inicio.press("Delete")
        page.fill(select_fecha_inicio_doom, "01/01/2026")
        time.sleep(2)
        #CAMBIAR TIPO DE REPORTE POR RESUMEN
        page.wait_for_selector("xpath=//html/body/kt-base/div/div/div/div/div/kt-horas-rpt/kt-portlet[1]/div/kt-portlet-body/div/form/div[1]/div/div[4]/div/mat-form-field/div[1]/div[2]", timeout=100000)
        page.locator("xpath=//html/body/kt-base/div/div/div/div/div/kt-horas-rpt/kt-portlet[1]/div/kt-portlet-body/div/form/div[1]/div/div[4]/div/mat-form-field/div[1]/div[2]").click()
        time.sleep(1)
        page.wait_for_selector("xpath=//html/body/div[3]/div[2]/div/div/mat-option[2]", timeout=100000)
        page.locator("xpath=//html/body/div[3]/div[2]/div/div/mat-option[2]").click()
        time.sleep(1)
        #"
        page.wait_for_selector("xpath=//html/body/kt-base/div/div/div/div/div/kt-horas-rpt/kt-portlet[1]/div/kt-portlet-header/div[2]/button", timeout=100000)
        page.locator("xpath=//html/body/kt-base/div/div/div/div/div/kt-horas-rpt/kt-portlet[1]/div/kt-portlet-header/div[2]/button").click()
        #page.locator("xpath=///html/body/kt-base/div/div/div/div/div/kt-horas-rpt/kt-portlet[1]/div/kt-portlet-header/div[2]/button").click()
        
        page.wait_for_selector("xpath=//html/body/kt-base/div/div/div/div/div/kt-horas-rpt/kt-portlet[2]/div/kt-portlet-body/div/div/ejs-grid/div[3]/div/table/tbody/tr[1]/td[3]", timeout=100000)
        page.locator("xpath=//html/body/kt-base/div/div/div/div/div/kt-horas-rpt/kt-portlet[2]/div/kt-portlet-body/div/div/ejs-grid/div[3]/div/table/tbody/tr[1]/td[3]")
        time.sleep(1)
        # Definir ruta de descarga
        # Definir ruta de descarga dinámica (compatible Linux/Windows)
        base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        download_folder = os.path.join(base_path, "data", "download")
        if not os.path.exists(download_folder):
            os.makedirs(download_folder)

        file_name = "QBERRIES.xlsx"
        save_path = os.path.join(download_folder, file_name)

        print(f"⬇️ Iniciando descarga... Esperando archivo en: {save_path}")
        
        # Esperar la descarga al hacer click QBERRIES
        with page.expect_download(timeout=900000) as download_info:
            page.locator("xpath=//html/body/kt-base/div/div/div/div/div/kt-horas-rpt/kt-portlet[2]/div/kt-portlet-header/div[2]/div/button").click()
        
        download = download_info.value
        print(f"✅ Descarga detectada: {download.suggested_filename}")
        
        # Guardar en el escritorio
        download.save_as(save_path)
        print(f"💾 Archivo guardado exitosamente en: {save_path}")
        index_empresa = {
            2:"EXCELLENCE.xlsx",
            3:"TARA FARM.xlsx",
            4:"GAP.xlsx",
            5:"CANYON.xlsx",
            6:"GOLDEN.xlsx",
            7:"BIG.xlsx"
        }

        for index,empresa in index_empresa.items():
            SELECT_EMPRESA = f"xpath=//html/body/div[3]/div[2]/div/div/mat-option[{index}]"
                                    
            page.locator("xpath=//html/body/kt-base/div/div/div/div/div/kt-horas-rpt/kt-portlet[1]/div/kt-portlet-body/div/form/div[1]/div/div[1]/div/mat-form-field/div[1]/div[2]/div/mat-select/div/div[1]").click()
            time.sleep(1)
            page.locator(SELECT_EMPRESA).click()
            time.sleep(1)
            
            # Click en botón de refrescar (Portlet 1)
            page.locator("xpath=//html/body/kt-base/div/div/div/div/div/kt-horas-rpt/kt-portlet[1]/div/kt-portlet-header/div[2]/button").click()
            
            print("⏳ Esperando 50 segundos para refresco de datos...")
            time.sleep(50)
            
            file_name = empresa
            save_path = os.path.join(download_folder, file_name)
            print(f"⬇️ Iniciando descarga... Esperando archivo en: {save_path}")
            
            # Esperar la descarga
            with page.expect_download(timeout=60000) as download_info:
                page.locator("xpath=//html/body/kt-base/div/div/div/div/div/kt-horas-rpt/kt-portlet[2]/div/kt-portlet-header/div[2]/div/button").click()
            
            download = download_info.value
            print(f"✅ Descarga detectada: {download.suggested_filename}")
            
            # Guardar
            download.save_as(save_path)
            print(f"💾 Archivo guardado exitosamente en: {save_path}")


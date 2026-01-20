# Usar Python 3.11 para coincidir con tu entorno local
FROM python:3.11-slim

# Establecer el directorio de trabajo
WORKDIR /app

# Copiar requirements.txt
COPY requirements.txt .

# Instalar dependencias de Python y del sistema para Playwright
# 1. Instalar librerías de Python
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 2. Instalar navegadores de Playwright y sus dependencias del sistema
# Se requiere 'playwright install --with-deps' para que funcione en linux/slim
RUN playwright install --with-deps chromium

# Copiar el resto del código
COPY . .

# Comando de inicio
CMD ["python", "schedule.py"]

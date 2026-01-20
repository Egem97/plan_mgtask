# Usar la imagen oficial de Playwright que incluye Python y los navegadores necesarios
FROM mcr.microsoft.com/playwright/python:v1.49.1-jammy

# Establecer el directorio de trabajo
WORKDIR /app

# Copiar requirements.txt
COPY requirements.txt .

# Instalar dependencias de Python
# Nota: La imagen ya trae playwright y navegadores compatibles con su versión interna.
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt


# Instalar navegadores explícitamente para asegurar compatibilidad con la versión de pip
RUN playwright install chromium

# Copiar el resto del código
COPY . .

# Comando de inicio
CMD ["python", "schedule.py"]

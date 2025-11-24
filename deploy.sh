#!/bin/bash

# Nombre de la imagen y del contenedor
IMAGE_NAME="schedule-app-image"
CONTAINER_NAME="schedule-app-container"

echo "🚀 Iniciando despliegue de $CONTAINER_NAME..."

# 1. Construir la imagen de Docker
echo "🔨 Construyendo la imagen..."
docker build -t $IMAGE_NAME .

# 2. Detener y eliminar el contenedor existente (si existe)
if [ "$(docker ps -aq -f name=$CONTAINER_NAME)" ]; then
    echo "🛑 Deteniendo y eliminando contenedor anterior..."
    docker stop $CONTAINER_NAME
    docker rm $CONTAINER_NAME
fi

# 3. Ejecutar el nuevo contenedor
echo "▶️ Iniciando nuevo contenedor..."
# -d: Detached mode (segundo plano)
# --restart always: Reiniciar automáticamente si falla o si se reinicia el servidor
# -v $(pwd):/app: Montar el directorio actual para persistencia y logs (opcional, pero útil para config.yaml)
# --name: Nombre del contenedor
docker run -d \
    --name $CONTAINER_NAME \
    --restart always \
    -v "$(pwd):/app" \
    $IMAGE_NAME

echo "✅ Despliegue completado exitosamente!"
echo "📜 Logs del contenedor:"
docker logs --tail 10 $CONTAINER_NAME

# Imagen base optimizada de Python
FROM python:3.11-slim-bullseye

# Instalación de dependencias del sistema y entorno Java para PySpark
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Definición de variables de entorno para Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Directorio de trabajo
WORKDIR /app

# Copia e instalación de dependencias de Python sin caché
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia del código fuente refactorizado
COPY src ./src

# Directorio de trabajo para la ejecución del pipeline (para soportar imports relativos)
WORKDIR /app/src/backend

# Comando de ejecución por defecto del pipeline
CMD ["python", "main.py"]

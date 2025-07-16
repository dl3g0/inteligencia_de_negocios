# Usa una imagen base de Python ligera con Debian Bullseye (Debian 11)
FROM python:3.9-slim-bullseye

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Instala dependencias necesarias para Azure CLI y el conector MySQL
# y para la gestión de certificados
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    build-essential \
    default-libmysqlclient-dev \
    && rm -rf /var/lib/apt/lists/*

# Instala Azure CLI dentro del contenedor
RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copia el archivo de requisitos e instala las dependencias de Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia el certificado SSL de Azure necesario para la conexión MySQL
# Asegúrate de que el nombre del archivo aquí coincida con el que descargaste
COPY DigiCertGlobalRootG2.crt.pem /usr/local/share/ca-certificates/DigiCertGlobalRootG2.crt.pem
RUN update-ca-certificates

# Copia el script ETL al directorio de trabajo
COPY etl.py .

# Comando para ejecutar el script ETL cuando el contenedor se inicie
CMD ["python", "etl.py"]

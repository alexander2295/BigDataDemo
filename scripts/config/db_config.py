# Proyecto-hive/scripts/config/db_config.py

import os
from dotenv import load_dotenv

# Cargar el archivo .env (está montado en el contenedor en /etc/credenciales/.env)
load_dotenv("/etc/credenciales/.env", override=True)

db_config = {
    "server": "host.docker.internal",  # desde los contenedores Docker
    "port": os.getenv("DB_PORT", "1433"),
    "database": os.getenv("DB_DATABASE"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "driver": os.getenv("DB_DRIVER"),
    "jdbc_url": os.getenv("DB_JDBC_URL")
}

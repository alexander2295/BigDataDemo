# -*- coding: utf-8 -*-

import sys
import os
from pyspark.sql import SparkSession

# Configurar la codificación para manejar caracteres especiales
if sys.version_info[0] == 2:
    reload(sys)
    sys.setdefaultencoding('utf-8')

# Configurar variables de entorno para UTF-8
os.environ['PYTHONIOENCODING'] = 'utf-8'

# Configuración de conexión
db_config = {
    "jdbc_url": "jdbc:sqlserver://host.docker.internal:1433;databaseName=olva;encrypt=false;characterEncoding=UTF-8",
    "database": "olva",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "user": "etl_user",
    "password": "StrongPass123",
    "port": 1433
}

# Crear sesión de Spark con configuración UTF-8
spark = SparkSession.builder \
    .appName("ExtractClientesToHDFS") \
    .config("spark.jars", "/opt/spark/jars/mssql-jdbc-13.2.0.jre8.jar") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

try:
    # Leer tabla Clientes desde SQL Server
    df_clientes = spark.read.jdbc(
        url=db_config["jdbc_url"],
        table="Clientes",
        properties={
            "user": db_config["user"],
            "password": db_config["password"],
            "driver": db_config["driver"],
            "characterEncoding": "UTF-8",
            "useUnicode": "true"
        }
    )

    print(u"Datos extraidos de SQL Server:".encode('utf-8'))
    
    # Mostrar datos sin truncar y manejando la codificación
    df_clientes.show(5, truncate=False)
    
    # Contar registros
    total_records = df_clientes.count()
    print(u"Total de registros: {}".format(total_records).encode('utf-8'))

    # Definir ruta en HDFS Bronze
    output_path = "hdfs://namenode:8020/bronze/clientes"

    # Guardar en formato Parquet con configuración UTF-8
    df_clientes.write \
        .mode("overwrite") \
        .option("encoding", "UTF-8") \
        .parquet(output_path)

    print(u"Datos guardados en HDFS Bronze en: {}".format(output_path).encode('utf-8'))

except Exception as e:
    print(u"Error durante la extraccion: {}".format(str(e)).encode('utf-8'))

finally:
    spark.stop()
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession

# Configuración de conexión (actualizada)
db_config = {
    "jdbc_url": "jdbc:sqlserver://host.docker.internal:1433;databaseName=olva;encrypt=false",
    "database": "olva",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "user": "etl_user",
    "password": "StrongPass123",
    "port": 1433
}

# Crear sesión de Spark con el driver JDBC
spark = SparkSession.builder \
    .appName("SQLServerConnectionTest") \
    .config("spark.jars", "/opt/spark/jars/mssql-jdbc-13.2.0.jre8.jar") \
    .getOrCreate()

try:
    # Intentar leer catálogo de tablas
    df = spark.read.jdbc(
        url=db_config["jdbc_url"],
        table="INFORMATION_SCHEMA.TABLES",
        properties={
            "user": db_config["user"],
            "password": db_config["password"],
            "driver": db_config["driver"]
        }
    )

    print("✅ Conexión establecida correctamente. Tablas en la BD:")
    df.select("TABLE_NAME").show(10, truncate=False)

except Exception as e:
    print("❌ Error al conectar con SQL Server:", str(e))

finally:
    spark.stop()

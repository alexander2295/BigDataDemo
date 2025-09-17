# -*- coding: utf-8 -*-

import os
import unicodedata
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, initcap, regexp_replace, length, when, udf
from pyspark.sql.types import StringType

# Configurar variables de entorno
os.environ['PYTHONIOENCODING'] = 'utf-8'

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("ProcessClientesBronzeToSilver") \
    .getOrCreate()

# UDF para limpiar acentos y caracteres especiales
def normalize_text(text):
    if text is None:
        return None
    # Normalizar (quita tildes y ñ → n)
    text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')
    return text

normalize_udf = udf(normalize_text, StringType())

try:
    # Rutas HDFS
    input_path = "hdfs://namenode:8020/bronze/clientes"
    output_path = "hdfs://namenode:8020/silver/clientes"

    # Leer Bronze
    df_bronze = spark.read.parquet(input_path)

    print("Datos leídos desde Bronze")
    print("Registros encontrados: {}".format(df_bronze.count()))

    # Transformaciones
    df_silver = df_bronze \
        .withColumn("Nombre", normalize_udf(initcap(trim(col("Nombre"))))) \
        .withColumn("Email", lower(trim(col("Email")))) \
        .withColumn("Telefono", regexp_replace(trim(col("Telefono")), "[^0-9]", "")) \
        .withColumn("UpdateTime", when(col("UpdateTime").isNull(), col("CreateTime")).otherwise(col("UpdateTime")))

    # Filtrar teléfonos inválidos
    df_silver = df_silver.filter((length(col("Telefono")) >= 7) & (length(col("Telefono")) <= 10))

    # Eliminar duplicados
    df_silver = df_silver.dropDuplicates(["ClienteID"])

    print("Procesamiento completado")
    print("Registros finales: {}".format(df_silver.count()))

    # Guardar en Silver
    df_silver.write.mode("overwrite").parquet(output_path)

    print("Datos guardados en: {}".format(output_path))

except Exception as e:
    print("Error: {}".format(str(e)))

finally:
    spark.stop()

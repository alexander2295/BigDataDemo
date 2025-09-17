# -*- coding: utf-8 -*-

import subprocess
import time
import sys
import os

# Configurar variables de entorno para UTF-8
os.environ['PYTHONIOENCODING'] = 'utf-8'

def run_spark_job(script_name):
    try:
        print("Ejecutando " + script_name + "...")
        
        # Para Python 2.7, usar subprocess.Popen en lugar de subprocess.run
        process = subprocess.Popen(
            [
                "/spark/bin/spark-submit",
                "--jars", "/opt/spark/jars/mssql-jdbc-13.2.0.jre8.jar",
                "/scripts/" + script_name
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        stdout, stderr = process.communicate()
        
        if process.returncode == 0:
            print(script_name + " completado con exito")
            if stdout:
                print(stdout)
        else:
            print("Error al ejecutar " + script_name + ":")
            if stderr:
                print(stderr)
            sys.exit(1)
            
    except Exception as e:
        print("Error inesperado con " + script_name + ": " + str(e))
        sys.exit(1)

if __name__ == "__main__":
    print("Iniciando pipeline de datos de clientes...")
    
    # Paso 1: Extracción desde SQL Server -> Bronze
    print("Paso 1: Extrayendo datos de SQL Server a Bronze...")
    run_spark_job("extract_clientes.py")
    
    # Pausa para asegurar que los archivos estén en HDFS
    print("Esperando 10 segundos antes de procesar Bronze -> Silver...")
    time.sleep(10)
    
    # Paso 2: Procesamiento Bronze -> Silver
    print("Paso 2: Procesando datos de Bronze a Silver...")
    run_spark_job("process_clientes.py")
    
    print("Pipeline completado correctamente. Datos disponibles en Silver.")
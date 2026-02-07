import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Configuração de Ambiente
caminho_projeto = os.path.dirname(os.path.abspath(__file__))
os.environ["HADOOP_HOME"] = os.path.join(caminho_projeto, "Hadoop")
os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep + os.environ["PATH"]

spark = SparkSession.builder \
    .appName("RefinoSilver") \
    .config("spark.master", "local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

try:
    # 1. LER DA BRONZE
    caminho_bronze = os.path.join(caminho_projeto, "data", "bronze", "checkins_teste")
    df_bronze = spark.read.parquet(caminho_bronze)

    print(f"✨ Iniciando refino de {df_bronze.count()} registros...")

    # 2. TRANSFORMAÇÃO (A mágica da Camada Silver)
    df_silver = df_bronze \
        .filter(F.col("valor") > 50.00) \
        .withColumn("categoria_vip", F.when(F.col("valor") > 80, "PREMIUM").otherwise("STANDARD")) \
        .withColumn("data_processamento", F.current_timestamp())

    # 3. SALVAR NA SILVER
    caminho_silver = os.path.join(caminho_projeto, "data", "silver", "checkins_vip")
    
    print("💾 Gravando dados limpos na Camada Silver...")
    df_silver.write.mode("overwrite").parquet(caminho_silver)

    print(f"✅ SUCESSO! Agora temos {df_silver.count()} registros filtrados na Silver.")
    df_silver.show(5)

except Exception as e:
    print(f"❌ ERRO: {e}")

finally:
    spark.stop()
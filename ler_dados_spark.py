import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. FORÇANDO O AMBIENTE (O Spark precisa disso para não dar erro de Link no Windows)
caminho_projeto = os.path.dirname(os.path.abspath(__file__))
hadoop_home = os.path.join(caminho_projeto, "Hadoop")
hadoop_bin = os.path.join(hadoop_home, "bin")

# Essas 2 linhas abaixo resolvem o erro UnsatisfiedLinkError
os.environ["HADOOP_HOME"] = hadoop_home
os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ["PATH"]

# 2. INICIANDO A SESSÃO
spark = SparkSession.builder \
    .appName("LeituraBigData") \
    .config("spark.master", "local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()

# Silenciando os avisos amarelos (WARN) para focar no resultado
spark.sparkContext.setLogLevel("ERROR")

# 3. CAMINHO DOS DADOS
# Verifique se o nome da pasta abaixo está IGUAL ao que foi criado (checkins_teste ou checkins_massivos)
caminho_bronze = os.path.join(caminho_projeto, "data", "bronze", "checkins_teste")

print(f"📖 Lendo dados de: {caminho_bronze}")

try:
    # Lendo a pasta Parquet
    df = spark.read.parquet(caminho_bronze)

    print("-" * 30)
    print("📊 RESUMO DOS DADOS LIDOS:")
    print(f"Total de Linhas: {df.count()}")
    
    # Mostrando a estrutura e os primeiros dados
    df.printSchema()
    df.show(10)
    print("-" * 30)

except Exception as e:
    print(f"❌ ERRO AO LER: {e}")

finally:
    spark.stop()
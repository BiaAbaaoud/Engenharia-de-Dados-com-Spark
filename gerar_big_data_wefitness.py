import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. LOCALIZAÇÃO DO PROJETO
caminho_projeto = os.path.dirname(os.path.abspath(__file__))
hadoop_home = os.path.join(caminho_projeto, "Hadoop")
hadoop_bin = os.path.join(hadoop_home, "bin")

# 2. DEFINIR VARIÁVEIS DE AMBIENTE (Crucial para o Windows encontrar o winutils e hadoop.dll)
os.environ["HADOOP_HOME"] = hadoop_home
os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ["PATH"]

# 3. INICIANDO O SPARK COM CONFIGURAÇÕES DE "BYPASS"
spark = SparkSession.builder \
    .appName("GeradorPetabyteScale") \
    .config("spark.master", "local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.sql.warehouse.dir", os.path.join(caminho_projeto, "spark-warehouse")) \
    .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol") \
    .getOrCreate()

# Silenciar logs inúteis e focar no erro
spark.sparkContext.setLogLevel("ERROR")

print(f"🚀 Tentando vencer o erro do Windows em: {caminho_projeto}")

try:
    # Gerando uma massa menor para teste rápido (100k linhas)
    df = spark.range(0, 100000) \
        .withColumn("id_checkin", F.expr("uuid()")) \
        .withColumn("data", F.current_date()) \
        .withColumn("valor", (F.rand() * 100).cast("decimal(10,2)"))

    # Caminho final
    pasta_saida = os.path.join(caminho_projeto, "data", "bronze", "checkins_teste")

    print("💾 Gravando dados... Alhamdulilah, agora vai!")
    
    # Tentativa de escrita simples
    df.write.mode("overwrite").parquet(pasta_saida)
    
    print(f"✅ SUCESSO! Arquivos gerados em: {pasta_saida}")
    print("Se a pasta não estiver mais vazia, você é oficialmente uma Engenheira Spark!")

except Exception as e:
    print(f"❌ O ERRO PERSISTE: \n{e}")

finally:
    spark.stop()
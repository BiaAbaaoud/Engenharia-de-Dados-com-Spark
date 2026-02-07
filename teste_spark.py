from pyspark.sql import SparkSession
import os

# 1. Configurando as variáveis de ambiente via código para facilitar
pasta_projeto = os.getcwd()
os.environ["HADOOP_HOME"] = os.path.join(pasta_projeto, "Hadoop")
os.environ["path"] += os.path.join(pasta_projeto, "Hadoop", "bin")

# 2. Iniciando a Sessão Spark
spark = SparkSession.builder \
    .appName("PrimeiroContatoBigData") \
    .master("local[*]") \
    .getOrCreate()

print("\n🚀 Spark iniciado com sucesso! Alhamdulillah.")

# 3. Criando dados de teste (Simulando 1 milhão de check-ins)
data = [{"id": i, "unidade": "Academia_" + str(i % 10), "valor": 29.90} for i in range(1000000)]
df = spark.createDataFrame(data)

# 4. Mostrando o poder do processamento
print(f"📊 Total de registros processados: {df.count()}")
df.show(5)

# 5. Encerrando a sessão
spark.stop()
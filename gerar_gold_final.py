import os
import sqlite3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Configuração de Ambiente para o Spark
caminho_projeto = os.path.dirname(os.path.abspath(__file__))
os.environ["HADOOP_HOME"] = os.path.join(caminho_projeto, "Hadoop")
os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep + os.environ["PATH"]

spark = SparkSession.builder \
    .appName("AgregacaoGoldParaSQL") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

try:
    # 2. LER DA SILVER
    caminho_silver = os.path.join(caminho_projeto, "data", "silver", "checkins_vip")
    if not os.path.exists(caminho_silver):
        raise Exception("A pasta Silver não foi encontrada! Rode o script anterior primeiro.")
    
    df_silver = spark.read.parquet(caminho_silver)

    print("✨ Criando visão executiva na Camada Gold...")

    # 3. AGREGRAÇÃO
    df_gold = df_silver.groupBy("categoria_vip").agg(
        F.count("id").alias("total_clientes"),
        F.sum("valor").alias("faturamento_total"),
        F.round(F.avg("valor"), 2).alias("ticket_medio")
    )

    # 4. SALVAR EM PARQUET (Backup na Camada Gold)
    caminho_gold = os.path.join(caminho_projeto, "data", "gold", "faturamento_por_categoria")
    df_gold.write.mode("overwrite").parquet(caminho_gold)
    
    print("📊 RESULTADO GERADO:")
    df_gold.show()

    # 5. SUBIR NO SQLITE (A ponte Spark -> Banco de Dados)
    print("🗄️ Enviando dados para o banco SQLite...")
    
    # Convertemos para Pandas para facilitar a inserção no SQLite
    pandas_df = df_gold.toPandas()
    
    # CORREÇÃO CRUCIAL: Converter Decimal para Float (SQLite não aceita Decimal)
    pandas_df['faturamento_total'] = pandas_df['faturamento_total'].astype(float)
    pandas_df['ticket_medio'] = pandas_df['ticket_medio'].astype(float)
    
    # Criamos a conexão e salvamos
    conn = sqlite3.connect('wefitness_analytics.db')
    pandas_df.to_sql('resumo_faturamento', conn, if_exists='replace', index=False)
    conn.close()
    
    print("✅ SUCESSO TOTAL! O banco 'wefitness_analytics.db' foi atualizado.")

except Exception as e:
    print(f"❌ OCORREU UM ERRO: {e}")

finally:
    # Finaliza a sessão Spark para liberar memória
    spark.stop()
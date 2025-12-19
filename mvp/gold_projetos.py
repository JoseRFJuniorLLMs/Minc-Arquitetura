import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

# 1. SETUP DE AMBIENTE
JAVA_PATH = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10"
SPARK_PATH = r"D:\spark"
HADOOP_PATH = r"D:\hadoop"

os.environ['JAVA_HOME'] = JAVA_PATH
os.environ['SPARK_HOME'] = SPARK_PATH
os.environ['HADOOP_HOME'] = HADOOP_PATH
os.environ['PATH'] = f"{JAVA_PATH}\\bin;{SPARK_PATH}\\bin;{HADOOP_PATH}\\bin;" + os.environ['PATH']


def criar_sessao_gold():
    return (SparkSession.builder
            .appName("MinC_Gold_Final")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
            .getOrCreate())


def processar_gold():
    spark = criar_sessao_gold()

    path_hub = "D:/lakehouse/silver/delta/hub_projeto"
    path_sat = "D:/lakehouse/silver/delta/sat_projeto"
    path_gold = "D:/lakehouse/gold/delta/dim_projetos_culturais"

    print("\n📀 Lendo dados da Silver...")
    df_hub = spark.read.format("delta").load(path_hub)
    df_sat = spark.read.format("delta").load(path_sat)

    # DEDUPLICAÇÃO
    window_spec = Window.partitionBy("hk_projeto").orderBy(F.col("load_date").desc())
    df_sat_latest = df_sat.withColumn("rn", F.row_number().over(window_spec)) \
        .filter(F.col("rn") == 1) \
        .drop("rn")

    print("🤝 Unindo tabelas e corrigindo tipos...")

    # Seleção e cast para garantir que valor_projeto seja numérico
    df_gold = df_hub.alias("h").join(df_sat_latest.alias("s"), "hk_projeto", "inner") \
        .select(
        F.col("h.PRONAC").alias("codigo_pronac"),
        F.col("s.nome").alias("nome_projeto"),
        F.col("s.valor_projeto").cast(DoubleType()).alias("valor_projeto"),
        F.col("s.load_date").alias("data_ultima_carga")
    )

    # 3. GRAVAÇÃO COM OVERWRITE SCHEMA (Resolve o erro de Merge Fields)
    print(f"✨ Gravando Camada Gold em: {path_gold}")
    (df_gold.write.format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")  # <--- ISSO RESOLVE SEU ERRO
     .save(path_gold))

    print("\n✅ PROCESSO FINALIZADO COM SUCESSO!")
    print("-" * 80)

    # Mostra o resultado ordenado pelos maiores valores
    df_gold.orderBy(F.col("valor_projeto").desc()).show(15, truncate=True)
    print("-" * 80)

    spark.stop()


if __name__ == "__main__":
    processar_gold()
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# 1. SETUP DE AMBIENTE
JAVA_PATH = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10"
SPARK_PATH = r"D:\spark"
HADOOP_PATH = r"D:\hadoop"

os.environ['JAVA_HOME'] = JAVA_PATH
os.environ['SPARK_HOME'] = SPARK_PATH
os.environ['HADOOP_HOME'] = HADOOP_PATH
os.environ['PATH'] = f"{JAVA_PATH}\\bin;{SPARK_PATH}\\bin;{HADOOP_PATH}\\bin;" + os.environ['PATH']


def criar_sessao_silver():
    return (SparkSession.builder
            .appName("MinC_Silver_Incremental")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
            .getOrCreate())


def upsert_hub(spark, df_novo, path_hub):
    if not os.path.exists(path_hub):
        print("🐣 Criando Hub pela primeira vez...")
        df_novo.write.format("delta").save(path_hub)
    else:
        print("🔄 Executando MERGE no Hub (Evitando duplicatas)...")
        dt_hub = DeltaTable.forPath(spark, path_hub)

        # Se hk_projeto for igual, não faz nada. Se for novo, insere.
        (dt_hub.alias("target")
         .merge(df_novo.alias("source"), "target.hk_projeto = source.hk_projeto")
         .whenNotMatchedInsertAll()
         .execute())


def processar_silver_incremental():
    spark = criar_sessao_silver()

    path_bronze = "D:/lakehouse/bronze/delta/salic_projetos"
    path_silver_hub = "D:/lakehouse/silver/delta/hub_projeto"
    path_silver_sat = "D:/lakehouse/silver/delta/sat_projeto"

    df_bronze = spark.read.format("delta").load(path_bronze)

    # Preparação dos dados com Hash Keys
    df_vault = df_bronze.withColumn("hk_projeto", F.md5(F.col("PRONAC").cast("string"))) \
        .withColumn("load_date", F.current_timestamp()) \
        .withColumn("record_source", F.lit("SALIC_CSV"))

    # --- CARGA DO HUB ---
    hub_data = df_vault.select("hk_projeto", "PRONAC", "load_date", "record_source").distinct()
    upsert_hub(spark, hub_data, path_silver_hub)

    # --- CARGA DO SATELLITE (Append Only para Histórico) ---
    print("📜 Adicionando registros ao Satellite (Histórico)...")
    sat_data = df_vault.select(
        "hk_projeto", "load_date", "nome",
        "valor_aprovado", "valor_projeto", "record_source"
    )
    # Satellites no Data Vault costumam ser Append para manter o histórico de mudanças
    sat_data.write.format("delta").mode("append").save(path_silver_sat)

    print(f"✅ Processo Incremental Finalizado!")
    spark.stop()


if __name__ == "__main__":
    processar_silver_incremental()
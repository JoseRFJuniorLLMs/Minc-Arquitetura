import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. SETUP DE AMBIENTE (Mesmo do anterior)
JAVA_PATH = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10"
SPARK_PATH = r"D:\spark"
HADOOP_PATH = r"D:\hadoop"

os.environ['JAVA_HOME'] = JAVA_PATH
os.environ['SPARK_HOME'] = SPARK_PATH
os.environ['HADOOP_HOME'] = HADOOP_PATH
os.environ['PATH'] = f"{JAVA_PATH}\\bin;{SPARK_PATH}\\bin;{HADOOP_PATH}\\bin;" + os.environ['PATH']


def criar_sessao_silver():
    return (SparkSession.builder
            .appName("MinC_Silver_DataVault")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
            .getOrCreate())


def processar_silver_vault():
    spark = criar_sessao_silver()

    # Caminhos
    path_bronze = "D:/lakehouse/bronze/delta/salic_projetos"
    path_silver_hub = "D:/lakehouse/silver/delta/hub_projeto"
    path_silver_sat = "D:/lakehouse/silver/delta/sat_projeto"

    print("📖 Lendo dados da Camada Bronze...")
    df_bronze = spark.read.format("delta").load(path_bronze)

    # --- DATA VAULT TRANSFORMATION ---

    # Criando a Hash Key (Padrão Data Vault) e normalizando a Business Key
    df_vault = df_bronze.withColumn("hk_projeto", F.md5(F.col("PRONAC").cast("string"))) \
        .withColumn("load_date", F.current_timestamp()) \
        .withColumn("record_source", F.lit("SALIC_CSV"))

    # 1. HUB PROJETO (Chaves únicas)
    print("🛠️ Criando Hub Projeto...")
    hub_projeto = df_vault.select("hk_projeto", "PRONAC", "load_date", "record_source").distinct()
    hub_projeto.write.format("delta").mode("overwrite").save(path_silver_hub)

    # 2. SATELLITE PROJETO (Atributos/Detalhes)
    print("🛠️ Criando Satellite Projeto...")
    # Aqui selecionamos as colunas descritivas
    sat_projeto = df_vault.select(
        "hk_projeto",
        "load_date",
        "nome",
        "valor_aprovado",
        "valor_projeto",
        "record_source"
    )
    sat_projeto.write.format("delta").mode("overwrite").save(path_silver_sat)

    print(f"✅ Camada Silver (Data Vault) populada com sucesso!")
    print(f"Hub salvo em: {path_silver_hub}")
    print(f"Sat salvo em: {path_silver_sat}")

    # Amostra do Hub
    hub_projeto.show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    processar_silver_vault()
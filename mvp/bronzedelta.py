import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ==============================================================================
# 1. CONFIGURAÇÃO DE AMBIENTE
# ==============================================================================
JAVA_PATH = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10"
SPARK_PATH = r"D:\spark"
HADOOP_PATH = r"D:\hadoop"

os.environ['JAVA_HOME'] = JAVA_PATH
os.environ['SPARK_HOME'] = SPARK_PATH
os.environ['HADOOP_HOME'] = HADOOP_PATH
os.environ['PATH'] = f"{JAVA_PATH}\\bin;{SPARK_PATH}\\bin;{HADOOP_PATH}\\bin;" + os.environ['PATH']


def criar_sessao_spark_delta():
    jvm_flags = (
        "-Djava.security.manager=allow "
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
    )

    # Adicionando as configurações específicas do Delta Lake
    return (SparkSession.builder
            .appName("MinC_Bronze_Delta")
            .config("spark.driver.extraJavaOptions", jvm_flags)
            .config("spark.executor.extraJavaOptions", jvm_flags)
            # 1. Ativa as extensões do Delta
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            # 2. Define onde os pacotes serão baixados (evita erro de permissão no C:)
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
            # 3. Fix para Windows
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
            .getOrCreate())


def processar_bronze_delta():
    print("🚀 Iniciando Spark com Delta Lake...")
    spark = criar_sessao_spark_delta()

    try:
        input_csv = "raw_salic_projetos.csv"
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_csv)

        # Adicionamos uma coluna de controle para saber quando o dado entrou
        df_bronze = df.withColumn("data_carga_bronze", F.current_timestamp())

        # Caminho da tabela Delta
        output_delta = "D:/lakehouse/bronze/delta/salic_projetos"

        print(f"💾 Gravando tabela DELTA em: {output_delta}")

        # Gravando em formato DELTA
        df_bronze.write.format("delta").mode("overwrite").save(output_delta)

        print(f"✅ TABELA DELTA CRIADA! Total: {df_bronze.count()} registros.")

        # Lendo para testar se o Delta está funcional
        df_check = spark.read.format("delta").load(output_delta)
        df_check.select("PRONAC", "nome", "data_carga_bronze").show(5, truncate=False)

    except Exception as e:
        print(f"💥 ERRO: {e}")
    finally:
        spark.stop()


if __name__ == "__main__":
    processar_bronze_delta()
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

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


def criar_sessao_spark():
    jvm_flags = (
        "-Djava.security.manager=allow "
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
        "--add-opens=java.base/java.io=ALL-UNNAMED "
        "--add-opens=java.base/java.net=ALL-UNNAMED "
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/java.util=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
        "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
    )

    # Usando parênteses para evitar erros de Syntax com "\"
    spark = (SparkSession.builder
             .appName("MinC_Bronze_Layer")
             .config("spark.sql.warehouse.dir", "file:///D:/lakehouse/warehouse")
             .config("spark.driver.extraJavaOptions", jvm_flags)
             .config("spark.executor.extraJavaOptions", jvm_flags)
             .config("spark.hadoop.fs.permissions.umask-mode", "000")
             .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
             .getOrCreate())
    return spark


def processar_bronze():
    print(f"🚀 Iniciando Spark...")
    spark = criar_sessao_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        input_csv = "raw_salic_projetos.csv"
        print(f"📖 Lendo {input_csv}...")

        # Leitura robusta
        df = (spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .option("multiLine", "true")
              .option("escape", '"')
              .csv(input_csv))

        output_path = "D:/lakehouse/bronze/salic_projetos"

        print(f"💾 Gravando Parquet em: {output_path}")

        # Transformação básica de Bronze
        df_bronze = df.withColumn("data_carga", F.current_timestamp())

        # Escrita ignorando bugs de DLL nativa do Windows
        df_bronze.write.mode("overwrite").parquet(output_path)

        print(f"\n✅ SUCESSO! Registros processados: {df_bronze.count()}")
        df_bronze.select("PRONAC", "nome").show(5, truncate=False)

    except Exception as e:
        print(f"💥 ERRO NO PROCESSAMENTO: {e}")
    finally:
        spark.stop()
        print("🛑 Sessão encerrada.")


if __name__ == "__main__":
    processar_bronze()
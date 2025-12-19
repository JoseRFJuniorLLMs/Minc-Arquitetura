import os
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

# 1. SETUP DE AMBIENTE
JAVA_PATH = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10"
SPARK_PATH = r"D:\spark"
HADOOP_PATH = r"D:\hadoop"

os.environ['JAVA_HOME'] = JAVA_PATH
os.environ['SPARK_HOME'] = SPARK_PATH
os.environ['HADOOP_HOME'] = HADOOP_PATH
os.environ['PATH'] = f"{JAVA_PATH}\\bin;{SPARK_PATH}\\bin;{HADOOP_PATH}\\bin;" + os.environ['PATH']


def criar_sessao_spark():
    return (SparkSession.builder
            .appName("Delta_Auditoria")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
            .getOrCreate())


def auditar_tabelas():
    spark = criar_sessao_spark()

    # Caminhos das tabelas
    tabelas = {
        "BRONZE": "D:/lakehouse/bronze/delta/salic_projetos",
        "SILVER_HUB": "D:/lakehouse/silver/delta/hub_projeto",
        "SILVER_SAT": "D:/lakehouse/silver/delta/sat_projeto"
    }

    for nome, path in tabelas.items():
        print(f"\n" + "=" * 50)
        print(f"📊 HISTÓRICO DA TABELA: {nome}")
        print("=" * 50)

        try:
            dt = DeltaTable.forPath(spark, path)

            # Mostra as versões, timestamps e operações (WRITE, MERGE, etc)
            dt.history().select(
                "version",
                "timestamp",
                "operation",
                "operationParameters.mode"
            ).orderBy("version", ascending=False).show(truncate=False)

        except Exception as e:
            print(f"⚠️ Erro ao ler histórico de {nome}: {e}")

    # --- EXEMPLO DE TIME TRAVEL ---
    print("\n🕒 TESTE DE TIME TRAVEL (Lendo a Versão 0 do Hub)...")
    try:
        # Aqui você pode escolher 'versionAsOf', 0 para ver como a tabela nasceu
        df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(tabelas["SILVER_HUB"])
        print(f"Registros na Versão 0: {df_v0.count()}")
        df_v0.show(3)
    except:
        print("Não foi possível realizar o Time Travel (talvez só exista uma versão).")

    spark.stop()


if __name__ == "__main__":
    auditar_tabelas()
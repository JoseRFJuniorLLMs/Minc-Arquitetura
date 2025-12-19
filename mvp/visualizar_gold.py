import os
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import pandas as pd

# 1. SETUP AMBIENTE (Mesmo do anterior)
JAVA_PATH = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10"
os.environ['JAVA_HOME'] = JAVA_PATH


def gerar_grafico():
    spark = (SparkSession.builder
             .appName("Visualizacao_Gold")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
             .getOrCreate())

    path_gold = "D:/lakehouse/gold/delta/dim_projetos_culturais"

    print("📊 Lendo dados da Gold para visualização...")
    df_gold = spark.read.format("delta").load(path_gold)

    # Pegar os top 10 projetos mais caros e converter para Pandas
    top_10_df = df_gold.orderBy("valor_projeto", ascending=False).limit(10).toPandas()

    # 2. CRIAÇÃO DO GRÁFICO
    plt.figure(figsize=(12, 6))

    # Criar barras
    bars = plt.barh(top_10_df['nome_projeto'], top_10_df['valor_projeto'], color='skyblue')

    plt.xlabel('Valor do Projeto (R$)')
    plt.ylabel('Nome do Projeto')
    plt.title('Top 10 Projetos Culturais por Valor - Camada Gold')
    plt.gca().invert_yaxis()  # Inverter para o maior ficar no topo

    # Adicionar os valores escritos ao lado das barras
    for bar in bars:
        width = bar.get_width()
        plt.text(width, bar.get_y() + bar.get_height() / 2,
                 f' R$ {width:,.2f}',
                 va='center', fontsize=10)

    plt.tight_layout()

    # Salvar e mostrar
    plt.savefig("top_10_projetos.png")
    print("✅ Gráfico salvo como 'top_10_projetos.png'!")
    plt.show()

    spark.stop()


if __name__ == "__main__":
    gerar_grafico()
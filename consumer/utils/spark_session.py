"""
Factory do SparkSession com configurações otimizadas para streaming local.
"""

from pyspark.sql import SparkSession


def criar_spark_session(app_name: str = "HidroEletricaStreaming") -> SparkSession:
    """
    Cria e retorna SparkSession configurada para:
    - Streaming Kafka
    - Parquet particionado (lakehouse local)
    - Serialização otimizada
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")

        # --- Kafka ---
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        )

        # --- Shuffle e paralelismo ---
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")

        # --- Streaming ---
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.schemaInference", "false")

        # --- Parquet / lakehouse ---
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.parquet.mergeSchema", "false")

        # --- Logs mais limpos ---
        .config("spark.sql.adaptive.enabled", "true")

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark

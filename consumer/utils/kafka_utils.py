"""
Helpers para leitura de streams Kafka no Spark.
"""

from pyspark.sql import SparkSession, DataFrame


def kafka_read_stream(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    starting_offsets: str = "latest",
) -> DataFrame:
    """
    Retorna um streaming DataFrame com as colunas raw do Kafka:
    key, value (bytes), topic, partition, offset, timestamp.
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 5000)   # back-pressure
        .load()
    )

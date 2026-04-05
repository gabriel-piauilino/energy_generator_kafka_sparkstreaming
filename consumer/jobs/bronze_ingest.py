"""
JOB: Bronze Ingest
──────────────────
Kafka → Bronze (raw JSON preservado, imutável)

Responsabilidades:
- Ler stream do Kafka
- Preservar value como string (sem transformação)
- Adicionar colunas de proveniência (offset, partition, kafka_ts)
- Escrever Parquet particionado por event_date
- Manter checkpoint para exactly-once
"""

import sys
import os

# Permite rodar como script sem install do pacote
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from consumer.utils.spark_session import criar_spark_session
from consumer.utils.kafka_utils import kafka_read_stream
from pyspark.sql import functions as F

# ─── Paths ────────────────────────────────────────────────
BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "lakehouse")
)
BRONZE_PATH     = os.path.join(BASE_DIR, "bronze")
CHECKPOINT_PATH = os.path.join(BASE_DIR, "checkpoints", "bronze")

import os
BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC             = os.environ.get("TOPIC", "energia-hidreletrica")
TRIGGER_SECONDS   = 120   # ↑ de 10s → 120s: 12× menos arquivos
RETENTION_DAYS    = 2     # Bronze retém apenas 2 dias (histórico mínimo)


def _limpar_bronze_antigo(bronze_path: str, retention_days: int):
    import shutil, datetime
    cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=retention_days)
    base = os.path.abspath(bronze_path)
    if not os.path.exists(base):
        return
    for d in os.listdir(base):
        if not d.startswith("ingest_date="):
            continue
        try:
            date_val = datetime.datetime.strptime(d.replace("ingest_date=", ""), "%Y-%m-%d")
            if date_val < cutoff:
                shutil.rmtree(os.path.join(base, d), ignore_errors=True)
                print(f"[BRONZE] Removido: {d}")
        except ValueError:
            pass


def main():
    spark = criar_spark_session("Bronze-Ingest")
    spark.conf.set("spark.sql.shuffle.partitions", "2")

    raw_df = kafka_read_stream(
        spark,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        topic=TOPIC,
        starting_offsets="earliest",
    )

    bronze_df = raw_df.select(
        F.col("value").cast("string").alias("raw_json"),
        F.col("key").cast("string").alias("kafka_key"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.to_date(F.col("timestamp")).alias("ingest_date"),
    ).coalesce(1)   # 1 arquivo por batch por partição de data

    batch_counter = [0]

    def write_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        (batch_df.write
            .format("parquet")
            .mode("append")
            .partitionBy("ingest_date")
            .save(BRONZE_PATH))
        batch_counter[0] += 1
        if batch_counter[0] % 5 == 0:
            _limpar_bronze_antigo(BRONZE_PATH, RETENTION_DAYS)

    query = (
        bronze_df.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .start()
    )

    print(f"[BRONZE] Streaming iniciado → {BRONZE_PATH}")
    query.awaitTermination()


if __name__ == "__main__":
    main()

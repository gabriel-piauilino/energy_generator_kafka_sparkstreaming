"""
JOB: Gold Aggregations
───────────────────────
Silver → Gold (KPIs por janelas temporais)

Estratégia: foreachBatch com agregação batch pura.
- Sem watermark: janelas são fechadas imediatamente no batch
- Cada micro-batch acumula novos arquivos Silver e recalcula KPIs
- Compatível com dados históricos e em tempo real
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from consumer.utils.spark_session import criar_spark_session
from pyspark.sql import functions as F, DataFrame, SparkSession

BASE_DIR        = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "lakehouse"))
SILVER_PATH     = os.path.join(BASE_DIR, "silver")
GOLD_PATH       = os.path.join(BASE_DIR, "gold")
CHECKPOINT_PATH = os.path.join(BASE_DIR, "checkpoints", "gold")

TRIGGER_SECONDS  = 30
RETENTION_DAYS   = 7
MAX_FILES        = 100   # arquivos Silver por trigger


# ── Limpeza antiga ────────────────────────────────────────────────────────────

def _limpar_gold_antigo(gold_path: str, retention_days: int):
    import datetime
    cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=retention_days)
    for sub in ["window_5min", "window_1h"]:
        sub_path = os.path.join(gold_path, sub)
        if not os.path.exists(sub_path):
            continue
        for part in os.listdir(sub_path):
            part_path = os.path.join(sub_path, part)
            if not os.path.isdir(part_path):
                continue
            for fname in os.listdir(part_path):
                fpath = os.path.join(part_path, fname)
                if os.path.isfile(fpath):
                    import datetime as dt
                    age = dt.datetime.utcnow() - dt.datetime.utcfromtimestamp(os.path.getmtime(fpath))
                    if age.days >= retention_days:
                        os.remove(fpath)


# ── Agregação batch pura (sem watermark) ─────────────────────────────────────

def aggregate_batch(batch_df: DataFrame, window_minutes: int) -> DataFrame:
    """
    Calcula KPIs por janela de tempo usando window() no batch estático.
    Funciona para dados históricos e em tempo real.
    """
    if batch_df.rdd.isEmpty():
        return batch_df.limit(0)

    window_spec = F.window("event_ts", f"{window_minutes} minutes")

    agg = (
        batch_df
        .groupBy(window_spec, "usina_id", "usina_nome")
        .agg(
            F.count("*").alias("total_eventos"),
            F.avg("potencia_mw").alias("potencia_media_mw"),
            F.max("potencia_mw").alias("potencia_max_mw"),
            F.min("potencia_mw").alias("potencia_min_mw"),
            F.stddev("potencia_mw").alias("potencia_stddev_mw"),
            F.avg("eficiencia").alias("eficiencia_media"),
            F.min("eficiencia").alias("eficiencia_minima"),
            F.avg("eficiencia_relativa").alias("eficiencia_relativa_media"),
            F.avg("oee").alias("oee_medio"),
            F.min("oee").alias("oee_minimo"),
            F.avg("heat_rate_kj_kwh").alias("heat_rate_medio"),
            F.max("heat_rate_kj_kwh").alias("heat_rate_max"),
            F.avg("indice_desgaste").alias("desgaste_medio"),
            F.max("indice_desgaste").alias("desgaste_maximo"),
            F.sum("energia_incremental_mwh").alias("energia_gerada_mwh"),
            F.avg("potencia_perdida_estimada_mw").alias("perda_media_mw"),
            F.sum(F.when(F.col("anomalia") == True, 1).otherwise(0)).alias("total_anomalias"),
            F.sum(F.when(F.col("status_operacional") == "MANUTENCAO", 1).otherwise(0)).alias("total_manutencao"),
            F.sum(F.when(F.col("nivel_alerta") == "CRITICO", 1).otherwise(0)).alias("total_criticos"),
            F.max("temperatura_turbina_c").alias("temperatura_max_c"),
            F.avg("temperatura_turbina_c").alias("temperatura_media_c"),
            F.max("vibracao_mm_s").alias("vibracao_max_mm_s"),
            F.avg("vibracao_mm_s").alias("vibracao_media_mm_s"),
            F.avg("pressao_carcaca_kpa").alias("pressao_media_kpa"),
            F.avg("vazao_m3_s").alias("vazao_media_m3_s"),
            F.max("vazao_m3_s").alias("vazao_max_m3_s"),
            F.avg("chuva_mm_h").alias("chuva_media_mm_h"),
            F.max("chuva_mm_h").alias("chuva_max_mm_h"),
            F.avg("fator_capacidade").alias("fator_capacidade_medio"),
            F.avg("potencia_pct_cap").alias("potencia_pct_cap_media"),
            F.avg("fator_potencia_calculado").alias("fator_potencia_medio"),
            F.avg("potencia_reativa_mvar").alias("potencia_reativa_media_mvar"),
            F.min("timestamp_utc").alias("primeiro_evento_ts"),
            F.max("timestamp_utc").alias("ultimo_evento_ts"),
        )
    )

    agg = (
        agg
        .withColumn("taxa_criticos_pct",
            F.round(F.when(F.col("total_eventos") > 0,
                F.col("total_criticos") * 100.0 / F.col("total_eventos")).otherwise(0.0), 2))
        .withColumn("score_saude",
            F.round(
                0.40 * F.col("oee_medio")
                + 0.30 * F.col("eficiencia_relativa_media")
                + 0.20 * (1.0 - F.col("desgaste_medio"))
                + 0.10 * (1.0 - F.least(F.lit(1.0), F.col("taxa_criticos_pct") / 100.0)),
                5))
        .withColumn("janela_minutos", F.lit(window_minutes))
        .withColumn("window_start", F.col("window.start").cast("string"))
        .withColumn("window_end",   F.col("window.end").cast("string"))
        .drop("window")
    )

    return agg


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    spark = criar_spark_session("Gold-Aggregations")
    spark.conf.set("spark.sql.shuffle.partitions", "2")

    silver_schema = spark.read.parquet(SILVER_PATH).schema

    silver_stream = (
        spark.readStream
        .schema(silver_schema)
        .option("maxFilesPerTrigger", MAX_FILES)
        .parquet(SILVER_PATH)
    )

    path_5min = os.path.join(GOLD_PATH, "window_5min")
    path_1h   = os.path.join(GOLD_PATH, "window_1h")
    batch_counter = [0]

    def process_batch(batch_df: DataFrame, batch_id: int):
        if batch_df.rdd.isEmpty():
            print(f"[GOLD] batch {batch_id} vazio — skip")
            return

        n = batch_df.count()
        print(f"[GOLD] batch {batch_id} → {n} registros Silver")

        # ── 5 minutos ──────────────────────────────────────────────────
        agg_5min = aggregate_batch(batch_df, window_minutes=5)
        rows_5 = agg_5min.count()
        if rows_5 > 0:
            (agg_5min.coalesce(1).write
                .format("parquet")
                .mode("append")
                .partitionBy("usina_id")
                .save(path_5min))
            print(f"[GOLD] 5min → {rows_5} janelas escritas")

        # ── 1 hora ─────────────────────────────────────────────────────
        agg_1h = aggregate_batch(batch_df, window_minutes=60)
        rows_1h = agg_1h.count()
        if rows_1h > 0:
            (agg_1h.coalesce(1).write
                .format("parquet")
                .mode("append")
                .partitionBy("usina_id")
                .save(path_1h))
            print(f"[GOLD] 1h   → {rows_1h} janelas escritas")

        # ── Limpeza periódica ───────────────────────────────────────────
        batch_counter[0] += 1
        if batch_counter[0] % 20 == 0:
            _limpar_gold_antigo(GOLD_PATH, RETENTION_DAYS)

    query = (
        silver_stream.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .start()
    )

    print(f"[GOLD] Stream iniciado → {GOLD_PATH}")
    query.awaitTermination()


if __name__ == "__main__":
    main()

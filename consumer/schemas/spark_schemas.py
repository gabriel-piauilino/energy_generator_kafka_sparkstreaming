"""
Schemas PySpark (StructType) para todos os níveis do pipeline.
Versão 3.0 — alinhada ao simulador Eletrobras.
"""

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# =========================================================
# Schema do evento raw (como chega do Kafka — JSON string)
# =========================================================

METADATA_SCHEMA = StructType([
    StructField("fonte",   StringType(), True),
    StructField("versao",  StringType(), True),
    StructField("topico",  StringType(), True),
])

EVENTO_SCHEMA = StructType([
    # Identificação
    StructField("event_id",         StringType(),  False),
    StructField("timestamp_utc",    StringType(),  False),
    StructField("simulacao_t",      IntegerType(), False),

    # Usina
    StructField("usina_id",              StringType(),  False),
    StructField("usina_nome",            StringType(),  True),
    StructField("rio",                   StringType(),  True),
    StructField("uf",                    StringType(),  True),
    StructField("num_unidades",          IntegerType(), True),
    StructField("potencia_instalada_mw", DoubleType(),  True),

    # Status
    StructField("status_operacional", StringType(), False),
    StructField("nivel_alerta",       StringType(), False),
    StructField("anomalia",           BooleanType(),False),
    StructField("tipo_anomalia",      StringType(), True),   # nullable

    # Hidráulico
    StructField("vazao_m3_s",          DoubleType(), False),
    StructField("vazao_bruta_m3_s",    DoubleType(), False),
    StructField("queda_bruta_m",       DoubleType(), False),
    StructField("pressao_carcaca_kpa", DoubleType(), True),

    # Elétrico
    StructField("eficiencia",              DoubleType(), False),
    StructField("potencia_mw",             DoubleType(), False),
    StructField("potencia_reativa_mvar",   DoubleType(), True),
    StructField("fator_capacidade",        DoubleType(), True),
    StructField("rendimento_hidraulico",   DoubleType(), True),

    # Energia
    StructField("energia_incremental_mwh", DoubleType(), False),
    StructField("energia_acumulada_mwh",   DoubleType(), False),

    # Ambiental / Mecânico
    StructField("chuva_mm_h",            DoubleType(), True),
    StructField("temperatura_turbina_c", DoubleType(), True),
    StructField("vibracao_mm_s",         DoubleType(), True),

    # Métricas avançadas
    StructField("oee",              DoubleType(), True),
    StructField("heat_rate_kj_kwh", DoubleType(), True),
    StructField("indice_desgaste",  DoubleType(), True),

    # Constantes
    StructField("densidade_agua_kg_m3", DoubleType(), True),
    StructField("gravidade_m_s2",       DoubleType(), True),

    # Metadados
    StructField("metadata", METADATA_SCHEMA, True),
])


# =========================================================
# Schema Silver — idêntico ao evento, mas com colunas extras derivadas
# (adicionadas via withColumn no job silver_transform)
# =========================================================
# Colunas derivadas adicionadas na Silver:
#   - event_date         : DateType (particionamento)
#   - event_hour         : IntegerType (0-23)
#   - event_ts           : TimestampType (para watermark)
#   - is_anomaly_critical: BooleanType
#   - eficiencia_cat     : StringType (ALTA / MEDIA / BAIXA)
#   - potencia_pct_cap   : DoubleType (fator_capacidade * 100)
#   - saldo_energia_mwh  : DoubleType (energia_acumulada no micro-batch)


# =========================================================
# Schema Gold — resultado das agregações por janela
# =========================================================
GOLD_WINDOW_SCHEMA = StructType([
    StructField("window_start",           StringType(), False),
    StructField("window_end",             StringType(), False),
    StructField("usina_id",               StringType(), False),
    StructField("usina_nome",             StringType(), True),
    StructField("total_eventos",          LongType(),   False),
    StructField("potencia_media_mw",      DoubleType(), True),
    StructField("potencia_max_mw",        DoubleType(), True),
    StructField("potencia_min_mw",        DoubleType(), True),
    StructField("eficiencia_media",       DoubleType(), True),
    StructField("oee_medio",              DoubleType(), True),
    StructField("heat_rate_medio",        DoubleType(), True),
    StructField("desgaste_medio",         DoubleType(), True),
    StructField("energia_gerada_mwh",     DoubleType(), True),
    StructField("total_anomalias",        LongType(),   True),
    StructField("total_manutencao",       LongType(),   True),
    StructField("taxa_criticos_pct",      DoubleType(), True),
    StructField("temperatura_max_c",      DoubleType(), True),
    StructField("temperatura_media_c",    DoubleType(), True),
    StructField("vibracao_max_mm_s",      DoubleType(), True),
    StructField("pressao_media_kpa",      DoubleType(), True),
    StructField("vazao_media_m3_s",       DoubleType(), True),
    StructField("chuva_media_mm_h",       DoubleType(), True),
    StructField("fator_capacidade_medio", DoubleType(), True),
    StructField("janela_minutos",         IntegerType(),False),
])

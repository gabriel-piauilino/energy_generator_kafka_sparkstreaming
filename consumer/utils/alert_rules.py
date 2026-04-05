"""
Regras de alerta centralizadas — aplicadas na Silver e consultadas no dashboard.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# Limites operacionais (ISO 10816 + práticas do setor elétrico)
LIMITES = {
    "temperatura_critica_c": 80.0,
    "temperatura_atencao_c": 72.0,
    "vibracao_critica_mm_s": 4.5,
    "vibracao_atencao_mm_s": 3.0,
    "eficiencia_critica":    0.75,
    "eficiencia_atencao":    0.83,
    "taxa_criticos_alerta_pct": 5.0,
    "desgaste_alerta":       0.65,
    "oee_minimo":            0.70,
}


def aplicar_flags_silver(df: DataFrame) -> DataFrame:
    """
    Adiciona colunas de alerta derivadas ao DataFrame Silver.
    """
    return df.withColumn(
        "is_anomaly_critical",
        (F.col("anomalia") == True) & (F.col("nivel_alerta") == "CRITICO"),
    ).withColumn(
        "eficiencia_cat",
        F.when(F.col("eficiencia") >= 0.88, "ALTA")
         .when(F.col("eficiencia") >= 0.80, "MEDIA")
         .otherwise("BAIXA"),
    ).withColumn(
        "potencia_pct_cap",
        F.round(F.col("fator_capacidade") * 100.0, 2),
    ).withColumn(
        "alerta_temperatura",
        F.when(F.col("temperatura_turbina_c") >= LIMITES["temperatura_critica_c"], "CRITICO")
         .when(F.col("temperatura_turbina_c") >= LIMITES["temperatura_atencao_c"], "ATENCAO")
         .otherwise("NORMAL"),
    ).withColumn(
        "alerta_vibracao",
        F.when(F.col("vibracao_mm_s") >= LIMITES["vibracao_critica_mm_s"], "CRITICO")
         .when(F.col("vibracao_mm_s") >= LIMITES["vibracao_atencao_mm_s"], "ATENCAO")
         .otherwise("NORMAL"),
    ).withColumn(
        "alerta_desgaste",
        F.when(F.col("indice_desgaste") >= LIMITES["desgaste_alerta"], "CRITICO")
         .otherwise("NORMAL"),
    )

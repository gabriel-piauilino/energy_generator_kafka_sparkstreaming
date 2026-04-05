# =========================================================
# ⚙️ CONFIGURAÇÕES GLOBAIS DO PRODUCER
# Lidas de variáveis de ambiente (docker-compose) com fallback
# para desenvolvimento local.
# =========================================================

import os
from typing import Any, Dict, List

def _float(key: str, default: float) -> float:
    try:
        return float(os.environ[key])
    except (KeyError, ValueError):
        return default

def _int(key: str, default: int) -> int:
    try:
        return int(os.environ[key])
    except (KeyError, ValueError):
        return default

# --- Kafka ---
BOOTSTRAP_SERVERS: str = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC: str             = os.environ.get("TOPIC", "energia-hidreletrica")

# --- Simulação ---
EVENTS_PER_SECOND:       int = _int("EVENTS_PER_SECOND", 60)
FLUSH_EVERY_N_MESSAGES:  int = _int("FLUSH_EVERY_N_MESSAGES", 50)
SIMULATION_STEP_SECONDS: int = _int("SIMULATION_STEP_SECONDS", 1)

# --- Probabilidades de eventos ---
ANOMALY_PROBABILITY:     float = _float("ANOMALY_PROBABILITY",     0.025)
MAINTENANCE_PROBABILITY: float = _float("MAINTENANCE_PROBABILITY", 0.003)

# --- Constantes físicas ---
WATER_DENSITY: float = 1000.0   # kg/m³
GRAVITY:       float = 9.81     # m/s²

# --- Log ---
ENABLE_PRETTY_LOG: bool = True

# =========================================================
# 🏭 USINAS ELETROBRAS — 5 MAIORES (dados baseados em relatórios ONS/ANEEL)
# =========================================================
#
# Itaipu        : 14.000 MW instalados, 700 m³/s por unidade x 20 unidades
# Belo Monte    : 11.233 MW, queda 87m, vazão outorgada 13.000 m³/s
# Tucuruí       : 8.370 MW, queda 72m, vazão 13.800 m³/s máx
# Jirau         : 3.750 MW, queda 18m, vazão 14.000 m³/s
# Angra 1/2     : térmica nuclear, fora do escopo hidro
# Santo Antônio : 3.568 MW, queda 15.5m, vazão 16.000 m³/s
#
# Nota: valores de head e flow são da faixa operacional, não nameplate absoluto.
# =========================================================

USINAS: List[Dict[str, Any]] = [
    {
        "id": "itaipu",
        "nome": "Itaipu Binacional",
        "rio": "Rio Paraná",
        "uf": "PR",
        "potencia_instalada_mw": 14000,
        "head_m": 118.4,            # queda líquida operacional média
        "max_flow_m3_s": 12600,     # vazão total máxima (20 unidades × 630)
        "base_efficiency": 0.933,   # turbinas Kaplan/Francis ajustadas
        "temperatura_base_c": 52.0,
        "vibracao_base_mm_s": 1.4,
        "num_unidades": 20,
    },
    {
        "id": "belo_monte",
        "nome": "Belo Monte",
        "rio": "Rio Xingu",
        "uf": "PA",
        "potencia_instalada_mw": 11233,
        "head_m": 87.0,
        "max_flow_m3_s": 13000,
        "base_efficiency": 0.926,
        "temperatura_base_c": 56.0,
        "vibracao_base_mm_s": 1.6,
        "num_unidades": 24,
    },
    {
        "id": "tucurui",
        "nome": "Tucuruí",
        "rio": "Rio Tocantins",
        "uf": "PA",
        "potencia_instalada_mw": 8370,
        "head_m": 72.0,
        "max_flow_m3_s": 13800,
        "base_efficiency": 0.921,
        "temperatura_base_c": 58.0,
        "vibracao_base_mm_s": 1.8,
        "num_unidades": 25,
    },
    {
        "id": "jirau",
        "nome": "Jirau",
        "rio": "Rio Madeira",
        "uf": "RO",
        "potencia_instalada_mw": 3750,
        "head_m": 18.5,
        "max_flow_m3_s": 14000,     # baixa queda, altíssima vazão (bulbo)
        "base_efficiency": 0.912,
        "temperatura_base_c": 55.0,
        "vibracao_base_mm_s": 2.1,  # turbinas bulbo têm mais vibração
        "num_unidades": 50,
    },
    {
        "id": "santo_antonio",
        "nome": "Santo Antônio",
        "rio": "Rio Madeira",
        "uf": "RO",
        "potencia_instalada_mw": 3568,
        "head_m": 15.5,
        "max_flow_m3_s": 16000,
        "base_efficiency": 0.908,
        "temperatura_base_c": 55.0,
        "vibracao_base_mm_s": 2.3,
        "num_unidades": 50,
    },
]

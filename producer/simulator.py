"""
Simulador de geração de energia hidrelétrica — Usinas Eletrobras
Publica eventos JSON no Kafka com modelos físicos realistas.

Pode ser iniciado diretamente OU ter seus parâmetros sobrescritos
pelo dashboard (via arquivo de config ou argumentos de linha).
"""

import json
import math
import random
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from kafka import KafkaProducer

# Garante imports corretos tanto via `python producer/simulator.py`
# quanto via `python -m producer.simulator` a partir da raiz do repo
import os as _os, sys as _sys
_sys.path.insert(0, _os.path.join(_os.path.dirname(__file__), ".."))

import producer.config as cfg


# =========================================================
# 🧠 ESTADO GLOBAL DA SIMULAÇÃO
# =========================================================

running: bool = True
message_count: int = 0

# Inicializa estado por usina
def _init_estados(usinas: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {
        u["id"]: {
            "energia_acumulada_mwh": 0.0,
            "em_manutencao": False,
            "ciclos_restantes_manutencao": 0,
            "ultima_anomalia": None,
            "disponibilidade_acumulada_ciclos": 0,   # ciclos operando
            "total_ciclos": 0,
        }
        for u in usinas
    }

estado_usinas: Dict[str, Dict[str, Any]] = _init_estados(cfg.USINAS)


# =========================================================
# 🎨 LOG
# =========================================================

def color(text: str, code: str) -> str:
    if not cfg.ENABLE_PRETTY_LOG:
        return text
    return f"\033[{code}m{text}\033[0m"


def log_event(evento: Dict[str, Any]) -> None:
    status = evento["status_operacional"]
    anomalia = evento["anomalia"]
    potencia = evento["potencia_mw"]
    usina = evento["usina_id"]
    vazao = evento["vazao_m3_s"]

    if status == "MANUTENCAO":
        prefix = color("[MANUT]  ", "33")
    elif anomalia:
        prefix = color("[ANOMAL] ", "31")
    else:
        prefix = color("[OK]     ", "32")

    print(
        f"{prefix} "
        f"{color(usina, '36'):>18} | "
        f"pot={potencia:>8.2f} MW | "
        f"vazao={vazao:>8.1f} m³/s | "
        f"ef={evento['eficiencia']:>5.3f} | "
        f"OEE={evento['oee']:>5.3f} | "
        f"temp={evento['temperatura_turbina_c']:>5.1f}°C | "
        f"vib={evento['vibracao_mm_s']:>4.2f}mm/s | "
        f"alerta={evento['nivel_alerta']}"
    )


# =========================================================
# 🛑 ENCERRAMENTO GRACIOSO
# =========================================================

def handle_shutdown(signum, frame):
    global running
    print(color("\n[SIMULADOR] Encerrando...", "35"))
    running = False


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


# =========================================================
# 🌦️ MODELOS FÍSICOS DE SIMULAÇÃO
# =========================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def fator_sazonalidade(t: int) -> float:
    """Ciclo anual de vazão (período úmido dez-mar / seco jun-set no Brasil)."""
    return 1.0 + 0.32 * math.sin(t / 90.0)


def chuva_mm_h(t: int, seed_offset: float = 0.0) -> float:
    """Chuva com ciclos de escala + pulsos convectivos."""
    base = 9 + 7 * math.sin((t / 42.0) + seed_offset)
    pulsos = max(0.0, 15 * math.sin((t / 11.0) + seed_offset / 2))
    ruido = random.uniform(-2.0, 2.0)
    return max(0.0, round(base + pulsos + ruido, 3))


def gerar_vazao(t: int, usina: Dict[str, Any], chuva: float, seed_offset: float) -> float:
    """
    Modelo hidrológico simplificado:
    - Base: 55% da capacidade máxima
    - Sazonalidade + ciclos de curto prazo
    - Resposta à chuva com lag de propagação
    - Ruído gaussiano
    """
    max_flow = usina["max_flow_m3_s"]
    base = max_flow * 0.55
    ciclo_curto = math.sin((t / 12.0) + seed_offset) * (max_flow * 0.10)
    ciclo_longo = math.sin((t / 65.0) + seed_offset) * (max_flow * 0.09)
    impacto_chuva = chuva * (max_flow / 1800.0) * 3.2   # proporcional à bacia
    ruido = random.gauss(0, max_flow * 0.008)
    vazao = (base + ciclo_curto + ciclo_longo + impacto_chuva + ruido) * fator_sazonalidade(t)
    return max(0.0, min(round(vazao, 2), max_flow))


def eficiencia_dinamica(vazao: float, usina: Dict[str, Any]) -> float:
    """
    Curva de eficiência em função da carga relativa.
    Ponto ótimo de operação das turbinas Francis/Kaplan: ~72% da carga.
    Turbinas bulbo (Jirau, Santo Antônio): ótimo em ~78%.
    """
    max_flow = usina["max_flow_m3_s"]
    base_eff = usina["base_efficiency"]
    carga = vazao / max_flow if max_flow > 0 else 0.0
    # usinas de baixa queda (bulbo) têm curva mais plana
    ponto_otimo = 0.78 if usina["head_m"] < 25 else 0.72
    largura = 0.030 if usina["head_m"] < 25 else 0.025
    curva = math.exp(-((carga - ponto_otimo) ** 2) / largura)
    ef = (base_eff - 0.04) + (0.05 * curva)
    return max(0.75, min(0.97, round(ef, 5)))


def calcular_potencia_mw(vazao_m3_s: float, head_m: float, eficiencia: float) -> float:
    """P = η × ρ × g × Q × H  (em Watts → converte para MW)"""
    return round(
        eficiencia * cfg.WATER_DENSITY * cfg.GRAVITY * vazao_m3_s * head_m / 1_000_000,
        4,
    )


def calcular_temperatura(
    usina: Dict[str, Any],
    eficiencia: float,
    anomalia: bool,
    tipo_anomalia: Optional[str],
) -> float:
    """
    Temperatura da turbina:
    T = T_base + calor_gerado_por_perda_mecanica + ruído + impacto de anomalia
    Calor de perda ≈ (1 - η) × P_nominal × fator_dissipacao
    """
    t_base = usina["temperatura_base_c"]
    ruido = random.uniform(-1.5, 1.5)
    carga_termica = (1.0 - eficiencia) * 42.0 if eficiencia > 0 else 0.0
    extra = 0.0
    if anomalia:
        if tipo_anomalia == "AQUECIMENTO":
            extra = random.uniform(10.0, 18.0)
        elif tipo_anomalia == "CAVITACAO":
            extra = random.uniform(4.0, 9.0)
        else:
            extra = random.uniform(2.0, 5.0)
    return round(t_base + carga_termica + ruido + extra, 2)


def calcular_vibracao(
    usina: Dict[str, Any],
    anomalia: bool,
    tipo_anomalia: Optional[str],
) -> float:
    """
    Vibração mecânica em mm/s (ISO 10816 para máquinas rotativas).
    Limites ISO: <2.3 OK, 2.3–4.5 ATENÇÃO, >4.5 CRÍTICO
    """
    vib = usina["vibracao_base_mm_s"] + random.uniform(-0.30, 0.30)
    if anomalia:
        if tipo_anomalia == "VIBRACAO_ALTA":
            vib += random.uniform(2.5, 5.0)
        elif tipo_anomalia == "CAVITACAO":
            vib += random.uniform(1.5, 3.2)
        else:
            vib += random.uniform(0.5, 1.5)
    return round(max(0.1, vib), 3)


def nivel_pressao_carcaca_kpa(vazao: float, max_flow: float, head_m: float) -> float:
    """
    Pressão estimada na carcaça da turbina (kPa).
    Simplificação: P ≈ ρ × g × H × (Q/Q_max)
    """
    razao = vazao / max_flow if max_flow > 0 else 0.0
    return round(cfg.WATER_DENSITY * cfg.GRAVITY * head_m * razao / 1000.0, 2)


# =========================================================
# ⚠️ LÓGICA OPERACIONAL (status, anomalias, manutenção)
# =========================================================

def aplicar_evento_operacional(
    usina_id: str,
    eficiencia: float,
    vazao: float,
    usina: Dict[str, Any],
) -> Dict[str, Any]:
    estado = estado_usinas[usina_id]
    estado["total_ciclos"] += 1

    # --- Manutenção em andamento ---
    if estado["em_manutencao"]:
        estado["ciclos_restantes_manutencao"] -= 1
        if estado["ciclos_restantes_manutencao"] <= 0:
            estado["em_manutencao"] = False
        return {
            "status_operacional": "MANUTENCAO",
            "eficiencia": 0.0,
            "vazao": 0.0,
            "anomalia": False,
            "tipo_anomalia": None,
        }

    # --- Novo evento de manutenção ---
    if random.random() < cfg.MAINTENANCE_PROBABILITY:
        estado["em_manutencao"] = True
        estado["ciclos_restantes_manutencao"] = random.randint(10, 30)
        return {
            "status_operacional": "MANUTENCAO",
            "eficiencia": 0.0,
            "vazao": 0.0,
            "anomalia": False,
            "tipo_anomalia": None,
        }

    estado["disponibilidade_acumulada_ciclos"] += 1

    # --- Anomalia ---
    if random.random() < cfg.ANOMALY_PROBABILITY:
        tipo = random.choice([
            "CAVITACAO",
            "AQUECIMENTO",
            "VIBRACAO_ALTA",
            "QUEDA_DE_RENDIMENTO",
            "RESTRICAO_HIDRAULICA",
        ])
        fators = {
            "CAVITACAO":           (random.uniform(0.72, 0.84), random.uniform(0.90, 0.98)),
            "AQUECIMENTO":         (random.uniform(0.75, 0.87), 1.0),
            "VIBRACAO_ALTA":       (random.uniform(0.78, 0.88), random.uniform(0.88, 0.96)),
            "QUEDA_DE_RENDIMENTO": (random.uniform(0.68, 0.82), 1.0),
            "RESTRICAO_HIDRAULICA":(random.uniform(0.82, 0.92), random.uniform(0.60, 0.82)),
        }
        fe, fv = fators[tipo]
        estado["ultima_anomalia"] = tipo
        return {
            "status_operacional": "OPERANDO_COM_DESVIO",
            "eficiencia": max(0.55, eficiencia * fe),
            "vazao": max(0.0, min(vazao * fv, usina["max_flow_m3_s"])),
            "anomalia": True,
            "tipo_anomalia": tipo,
        }

    estado["ultima_anomalia"] = None
    return {
        "status_operacional": "OPERANDO",
        "eficiencia": eficiencia,
        "vazao": vazao,
        "anomalia": False,
        "tipo_anomalia": None,
    }


def classificar_alerta(
    temperatura_c: float,
    vibracao_mm_s: float,
    eficiencia: float,
    anomalia: bool,
    manutencao: bool,
) -> str:
    if manutencao:
        return "INFO"
    if anomalia or temperatura_c >= 80 or vibracao_mm_s >= 4.5 or eficiencia < 0.75:
        return "CRITICO"
    if temperatura_c >= 72 or vibracao_mm_s >= 3.0 or eficiencia < 0.83:
        return "ATENCAO"
    return "NORMAL"


# =========================================================
# 📐 MÉTRICAS AVANÇADAS (calculadas no producer para enriquecer o evento)
# =========================================================

def calcular_oee(usina_id: str, potencia_mw: float, usina: Dict[str, Any]) -> float:
    """
    OEE (Overall Equipment Effectiveness) simplificado:
    OEE = Disponibilidade × Performance × Qualidade
    - Disponibilidade: ciclos operando / total de ciclos
    - Performance: potência atual / potência nominal
    - Qualidade: fixa em 1.0 (sem produto refugado em hidrelétricas)
    """
    estado = estado_usinas[usina_id]
    total = estado["total_ciclos"]
    if total == 0:
        return 0.0
    disponibilidade = estado["disponibilidade_acumulada_ciclos"] / total
    performance = min(1.0, potencia_mw / usina["potencia_instalada_mw"])
    return round(disponibilidade * performance * 1.0, 5)


def calcular_heat_rate_kj_kwh(eficiencia: float) -> float:
    """
    Equivalente hidráulico de heat rate: energia potencial por energia gerada.
    Para hidrelétricas: HR = 3600 / η  (kJ/kWh)
    Quanto menor, mais eficiente.
    """
    if eficiencia <= 0:
        return 0.0
    return round(3600.0 / eficiencia, 2)


def calcular_indice_desgaste(vibracao: float, temperatura: float, anomalia: bool) -> float:
    """
    Índice de desgaste composto [0..1]:
    Combina vibração (peso 60%) e temperatura (peso 40%).
    Normalizado por limites críticos ISO 10816 e limite térmico operacional.
    """
    idx_vib = min(1.0, vibracao / 4.5)          # limite crítico ISO: 4.5 mm/s
    idx_temp = min(1.0, max(0.0, (temperatura - 50.0) / 40.0))  # 50°C base, 90°C limite
    bonus_anomalia = 0.15 if anomalia else 0.0
    return round(min(1.0, 0.60 * idx_vib + 0.40 * idx_temp + bonus_anomalia), 5)


def calcular_potencia_reativa_mvar(potencia_mw: float, fator_potencia: float = 0.95) -> float:
    """
    Potência reativa estimada (Q = P × tan(arccos(FP))).
    Geradores síncronos de hidrelétrica operam tipicamente com FP ~0.95.
    """
    if fator_potencia <= 0 or fator_potencia >= 1:
        return 0.0
    return round(potencia_mw * math.tan(math.acos(fator_potencia)), 4)


def calcular_rendimento_hidraulico(
    potencia_mw: float,
    vazao: float,
    head_m: float,
) -> float:
    """
    Rendimento hidráulico real = P_gerada / P_hidraulica_disponível
    P_hidraul = ρ × g × Q × H
    """
    p_hidraulica = cfg.WATER_DENSITY * cfg.GRAVITY * vazao * head_m / 1_000_000
    if p_hidraulica <= 0:
        return 0.0
    return round(min(1.0, potencia_mw / p_hidraulica), 5)


# =========================================================
# 🎲 GERAÇÃO DO EVENTO COMPLETO
# =========================================================

def gerar_evento(usina: Dict[str, Any], t: int) -> Dict[str, Any]:
    uid = usina["id"]
    seed_offset = (hash(uid) % 17) / 5.0

    chuva = chuva_mm_h(t, seed_offset=seed_offset)
    vazao_bruta = gerar_vazao(t, usina, chuva, seed_offset)
    eficiencia_base = eficiencia_dinamica(vazao_bruta, usina)

    op = aplicar_evento_operacional(uid, eficiencia_base, vazao_bruta, usina)

    vazao_final = op["vazao"]
    eficiencia_final = op["eficiencia"]
    anomalia = op["anomalia"]
    tipo_anomalia = op["tipo_anomalia"]
    status = op["status_operacional"]

    potencia_mw = calcular_potencia_mw(vazao_final, usina["head_m"], eficiencia_final)
    energia_incremental_mwh = potencia_mw * (cfg.SIMULATION_STEP_SECONDS / 3600.0)
    estado_usinas[uid]["energia_acumulada_mwh"] += energia_incremental_mwh

    temperatura = calcular_temperatura(usina, eficiencia_final, anomalia, tipo_anomalia)
    vibracao = calcular_vibracao(usina, anomalia, tipo_anomalia)
    nivel_alerta = classificar_alerta(
        temperatura, vibracao, eficiencia_final, anomalia,
        manutencao=(status == "MANUTENCAO"),
    )

    potencia_max = calcular_potencia_mw(
        usina["max_flow_m3_s"], usina["head_m"], usina["base_efficiency"]
    )
    fator_capacidade = round(potencia_mw / potencia_max, 5) if potencia_max > 0 else 0.0

    # --- Métricas avançadas ---
    oee = calcular_oee(uid, potencia_mw, usina)
    heat_rate = calcular_heat_rate_kj_kwh(eficiencia_final)
    indice_desgaste = calcular_indice_desgaste(vibracao, temperatura, anomalia)
    potencia_reativa = calcular_potencia_reativa_mvar(potencia_mw)
    rendimento_hidraulico = calcular_rendimento_hidraulico(potencia_mw, vazao_final, usina["head_m"])
    pressao_carcaca_kpa = nivel_pressao_carcaca_kpa(vazao_final, usina["max_flow_m3_s"], usina["head_m"])

    evento = {
        # --- Identificação ---
        "event_id": f"{uid}-{t}-{int(time.time() * 1000)}",
        "timestamp_utc": utc_now_iso(),
        "simulacao_t": t,

        # --- Usina ---
        "usina_id": uid,
        "usina_nome": usina["nome"],
        "rio": usina["rio"],
        "uf": usina["uf"],
        "num_unidades": usina["num_unidades"],
        "potencia_instalada_mw": usina["potencia_instalada_mw"],

        # --- Status operacional ---
        "status_operacional": status,
        "nivel_alerta": nivel_alerta,
        "anomalia": anomalia,
        "tipo_anomalia": tipo_anomalia,

        # --- Medidas hidráulicas ---
        "vazao_m3_s": round(vazao_final, 2),
        "vazao_bruta_m3_s": round(vazao_bruta, 2),
        "queda_bruta_m": round(usina["head_m"], 2),
        "pressao_carcaca_kpa": pressao_carcaca_kpa,

        # --- Desempenho elétrico ---
        "eficiencia": round(eficiencia_final, 5),
        "potencia_mw": round(potencia_mw, 4),
        "potencia_reativa_mvar": potencia_reativa,
        "fator_capacidade": fator_capacidade,
        "rendimento_hidraulico": rendimento_hidraulico,

        # --- Energia ---
        "energia_incremental_mwh": round(energia_incremental_mwh, 6),
        "energia_acumulada_mwh": round(estado_usinas[uid]["energia_acumulada_mwh"], 4),

        # --- Condições ambientais e mecânicas ---
        "chuva_mm_h": round(chuva, 3),
        "temperatura_turbina_c": temperatura,
        "vibracao_mm_s": vibracao,

        # --- Métricas avançadas de saúde e desempenho ---
        "oee": oee,
        "heat_rate_kj_kwh": heat_rate,
        "indice_desgaste": indice_desgaste,

        # --- Constantes físicas usadas ---
        "densidade_agua_kg_m3": cfg.WATER_DENSITY,
        "gravidade_m_s2": cfg.GRAVITY,

        # --- Metadados ---
        "metadata": {
            "fonte": "simulador_hidreletrico_eletrobras",
            "versao": "3.0",
            "topico": cfg.TOPIC,
        },
    }

    return evento


# =========================================================
# 🚀 KAFKA PRODUCER
# =========================================================

def criar_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=cfg.BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        linger_ms=20,
        retries=5,
        max_in_flight_requests_per_connection=5,
    )


def on_send_error(excp):
    print(color(f"[ERRO KAFKA] {excp}", "31"), file=sys.stderr)


# =========================================================
# 🔁 LOOP PRINCIPAL
# =========================================================

def main() -> None:
    global message_count, running

    if cfg.EVENTS_PER_SECOND <= 0:
        raise ValueError("EVENTS_PER_SECOND precisa ser > 0")

    intervalo = 1.0 / cfg.EVENTS_PER_SECOND
    t = 0

    producer = criar_producer()

    print(color("=" * 70, "35"))
    print(color(" SIMULADOR HIDRELÉTRICO ELETROBRAS — Kafka Streaming", "35"))
    print(color(f" Bootstrap : {cfg.BOOTSTRAP_SERVERS}", "35"))
    print(color(f" Tópico    : {cfg.TOPIC}", "35"))
    print(color(f" EPS       : {cfg.EVENTS_PER_SECOND}  |  Usinas: {len(cfg.USINAS)}", "35"))
    print(color("=" * 70, "35"))

    try:
        while running:
            inicio_ciclo = time.perf_counter()

            for usina in cfg.USINAS:
                evento = gerar_evento(usina, t)

                future = producer.send(cfg.TOPIC, key=evento["usina_id"], value=evento)
                future.add_errback(on_send_error)

                log_event(evento)
                message_count += 1

                if message_count % cfg.FLUSH_EVERY_N_MESSAGES == 0:
                    producer.flush()

            t += cfg.SIMULATION_STEP_SECONDS
            duracao = time.perf_counter() - inicio_ciclo
            time.sleep(max(0.0, intervalo - duracao))

    except Exception as exc:
        print(color(f"[FALHA] {exc}", "31"), file=sys.stderr)
        raise
    finally:
        try:
            producer.flush()
            producer.close()
        except Exception:
            pass
        print(color("[SIMULADOR] Encerrado.", "35"))


if __name__ == "__main__":
    main()

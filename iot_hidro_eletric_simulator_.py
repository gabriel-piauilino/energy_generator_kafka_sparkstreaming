import json
import math
import random
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Dict, Any, List

from kafka import KafkaProducer


# =========================================================
# ⚙️ CONFIGURAÇÕES GERAIS
# =========================================================

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "energia-hidreletrica"

EVENTS_PER_SECOND = 180
FLUSH_EVERY_N_MESSAGES = 50

WATER_DENSITY = 1000       # kg/m³
GRAVITY = 10.81             # m/s²

SIMULATION_STEP_SECONDS = 1
ANOMALY_PROBABILITY = 0.025
MAINTENANCE_PROBABILITY = 0.003

ENABLE_PRETTY_LOG = True


# =========================================================
# 🏭 USINAS
# =========================================================

USINAS: List[Dict[str, Any]] = [
    {
        "id": "hidro-01",
        "nome": "Usina Serra Azul",
        "rio": "Rio Grande",
        "head_m": 105,
        "max_flow_m3_s": 520,
        "base_efficiency": 0.91,
        "temperatura_base_c": 58.0,
        "vibracao_base_mm_s": 1.8,
    },
    {
        "id": "hidro-02",
        "nome": "Usina Vale Verde",
        "rio": "Rio Paranaíba",
        "head_m": 82,
        "max_flow_m3_s": 340,
        "base_efficiency": 0.89,
        "temperatura_base_c": 55.0,
        "vibracao_base_mm_s": 1.5,
    },
    {
        "id": "hidro-03",
        "nome": "Usina Pedra Clara",
        "rio": "Rio São Francisco",
        "head_m": 128,
        "max_flow_m3_s": 710,
        "base_efficiency": 0.92,
        "temperatura_base_c": 60.0,
        "vibracao_base_mm_s": 2.0,
    },
    {
        "id": "hidro-04",
        "nome": "Usina Horizonte",
        "rio": "Rio Tocantins",
        "head_m": 97,
        "max_flow_m3_s": 460,
        "base_efficiency": 0.90,
        "temperatura_base_c": 57.0,
        "vibracao_base_mm_s": 1.7,
    },
]


# =========================================================
# 🎨 LOG BONITINHO
# =========================================================

def color(text: str, code: str) -> str:
    if not ENABLE_PRETTY_LOG:
        return text
    return f"\033[{code}m{text}\033[0m"


def log_event(evento: Dict[str, Any]) -> None:
    status = evento["status_operacional"]
    anomalia = evento["anomalia"]
    potencia = evento["potencia_mw"]
    usina = evento["usina_id"]
    vazao = evento["vazao_m3_s"]

    if status == "MANUTENCAO":
        prefix = color("[MANUTENCAO]", "33")
    elif anomalia:
        prefix = color("[ANOMALIA]", "31")
    else:
        prefix = color("[OK]", "32")

    print(
        f"{prefix} "
        f"{color(usina, '36')} | "
        f"pot={potencia:>7.2f} MW | "
        f"vazao={vazao:>7.2f} m3/s | "
        f"ef={evento['eficiencia']:>5.3f} | "
        f"temp={evento['temperatura_turbina_c']:>5.1f} °C | "
        f"vib={evento['vibracao_mm_s']:>4.2f} mm/s | "
        f"chuva={evento['chuva_mm_h']:>5.2f} mm/h | "
        f"energia_acum={evento['energia_acumulada_mwh']:>9.3f} MWh"
    )


# =========================================================
# 🧠 ESTADO GLOBAL DA SIMULAÇÃO
# =========================================================

running = True
message_count = 0

estado_usinas: Dict[str, Dict[str, Any]] = {
    usina["id"]: {
        "energia_acumulada_mwh": 0.0,
        "em_manutencao": False,
        "ciclos_restantes_manutencao": 0,
        "ultima_anomalia": None,
    }
    for usina in USINAS
}


# =========================================================
# 🛑 ENCERRAMENTO GRACIOSO
# =========================================================

def handle_shutdown(signum, frame):
    global running
    print(color("\nEncerrando simulador... sem explodir o Kafka, calma.", "35"))
    running = False


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


# =========================================================
# 🚀 KAFKA
# =========================================================

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    acks="all",
    linger_ms=20,
    retries=5,
    max_in_flight_requests_per_connection=5,
)


# =========================================================
# 🌦️ MODELOS DE SIMULAÇÃO
# =========================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def fator_chuva_global(t: int) -> float:
    """
    Oscilação lenta representando período mais úmido/seco.
    """
    return 1.0 + 0.28 * math.sin(t / 90.0)


def chuva_mm_h(t: int, seed_offset: float = 0.0) -> float:
    """
    Simula chuva com ciclos + pulsos.
    """
    base = 8 + 6 * math.sin((t / 40.0) + seed_offset)
    pulsos = max(0.0, 12 * math.sin((t / 13.0) + seed_offset / 2))
    ruido = random.uniform(-1.5, 1.5)
    return max(0.0, base + pulsos + ruido)


def gerar_vazao(t: int, max_flow: float, chuva: float, seed_offset: float = 0.0) -> float:
    """
    Vazão realista:
    - componente base
    - sazonalidade contínua
    - influência da chuva
    - ruído gaussiano
    """
    base = max_flow * 0.52
    ciclo_curto = math.sin((t / 10.0) + seed_offset) * (max_flow * 0.12)
    ciclo_longo = math.sin((t / 60.0) + seed_offset) * (max_flow * 0.08)
    impacto_chuva = chuva * 2.8
    ruido = random.gauss(0, max_flow * 0.01)

    vazao = (base + ciclo_curto + ciclo_longo + impacto_chuva + ruido) * fator_chuva_global(t)
    return max(0.0, min(vazao, max_flow))


def eficiencia_dinamica(vazao: float, max_flow: float, base_efficiency: float) -> float:
    """
    Eficiência costuma ser melhor próxima de uma faixa ótima de operação.
    """
    carga = 0.0 if max_flow == 0 else vazao / max_flow
    curva_otima = math.exp(-((carga - 0.72) ** 2) / 0.025)
    eficiencia = (base_efficiency - 0.05) + (0.06 * curva_otima)
    return max(0.72, min(0.96, eficiencia))


def sortear_manutencao() -> bool:
    return random.random() < MAINTENANCE_PROBABILITY


def sortear_anomalia() -> bool:
    return random.random() < ANOMALY_PROBABILITY


def aplicar_evento_operacional(
    usina_id: str,
    eficiencia: float,
    vazao: float,
    max_flow: float,
) -> Dict[str, Any]:
    """
    Ajusta operação por manutenção ou anomalia.
    """
    estado = estado_usinas[usina_id]

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

    if sortear_manutencao():
        estado["em_manutencao"] = True
        estado["ciclos_restantes_manutencao"] = random.randint(8, 20)
        return {
            "status_operacional": "MANUTENCAO",
            "eficiencia": 0.0,
            "vazao": 0.0,
            "anomalia": False,
            "tipo_anomalia": None,
        }

    if not sortear_anomalia():
        estado["ultima_anomalia"] = None
        return {
            "status_operacional": "OPERANDO",
            "eficiencia": eficiencia,
            "vazao": vazao,
            "anomalia": False,
            "tipo_anomalia": None,
        }

    tipo = random.choice([
        "CAVITACAO",
        "AQUECIMENTO",
        "VIBRACAO_ALTA",
        "QUEDA_DE_RENDIMENTO",
        "RESTRICAO_HIDRAULICA",
    ])

    if tipo == "CAVITACAO":
        eficiencia *= random.uniform(0.72, 0.84)
        vazao *= random.uniform(0.90, 0.98)
    elif tipo == "AQUECIMENTO":
        eficiencia *= random.uniform(0.75, 0.87)
    elif tipo == "VIBRACAO_ALTA":
        eficiencia *= random.uniform(0.78, 0.88)
        vazao *= random.uniform(0.88, 0.96)
    elif tipo == "QUEDA_DE_RENDIMENTO":
        eficiencia *= random.uniform(0.68, 0.82)
    elif tipo == "RESTRICAO_HIDRAULICA":
        vazao *= random.uniform(0.60, 0.82)
        eficiencia *= random.uniform(0.82, 0.92)

    estado["ultima_anomalia"] = tipo

    return {
        "status_operacional": "OPERANDO_COM_DESVIO",
        "eficiencia": max(0.55, eficiencia),
        "vazao": max(0.0, min(vazao, max_flow)),
        "anomalia": True,
        "tipo_anomalia": tipo,
    }


def calcular_potencia_mw(vazao_m3_s: float, head_m: float, eficiencia: float) -> float:
    potencia_w = eficiencia * WATER_DENSITY * GRAVITY * vazao_m3_s * head_m
    return potencia_w / 1_000_000


def calcular_temperatura(temperatura_base: float, eficiencia: float, anomalia: bool, tipo_anomalia: str | None) -> float:
    temp = temperatura_base + random.uniform(-1.2, 1.2)

    if eficiencia > 0:
        carga_termica = (1 - eficiencia) * 40
        temp += carga_termica

    if anomalia:
        if tipo_anomalia == "AQUECIMENTO":
            temp += random.uniform(8, 15)
        else:
            temp += random.uniform(2, 6)

    return round(temp, 2)


def calcular_vibracao(vibracao_base: float, anomalia: bool, tipo_anomalia: str | None) -> float:
    vib = vibracao_base + random.uniform(-0.25, 0.25)

    if anomalia:
        if tipo_anomalia == "VIBRACAO_ALTA":
            vib += random.uniform(2.0, 4.5)
        elif tipo_anomalia == "CAVITACAO":
            vib += random.uniform(1.2, 2.8)
        else:
            vib += random.uniform(0.4, 1.2)

    return round(max(0.2, vib), 3)


def classificar_alerta(
    temperatura_c: float,
    vibracao_mm_s: float,
    eficiencia: float,
    anomalia: bool,
    manutencao: bool,
) -> str:
    if manutencao:
        return "INFO"

    if anomalia or temperatura_c >= 78 or vibracao_mm_s >= 4.5 or eficiencia < 0.75:
        return "CRITICO"

    if temperatura_c >= 70 or vibracao_mm_s >= 3.0 or eficiencia < 0.82:
        return "ATENCAO"

    return "NORMAL"


def gerar_evento(usina: Dict[str, Any], t: int) -> Dict[str, Any]:
    usina_id = usina["id"]
    seed_offset = (hash(usina_id) % 17) / 5.0

    chuva = chuva_mm_h(t, seed_offset=seed_offset)
    vazao = gerar_vazao(
        t=t,
        max_flow=usina["max_flow_m3_s"],
        chuva=chuva,
        seed_offset=seed_offset,
    )

    eficiencia = eficiencia_dinamica(
        vazao=vazao,
        max_flow=usina["max_flow_m3_s"],
        base_efficiency=usina["base_efficiency"],
    )

    operacao = aplicar_evento_operacional(
        usina_id=usina_id,
        eficiencia=eficiencia,
        vazao=vazao,
        max_flow=usina["max_flow_m3_s"],
    )

    vazao_final = operacao["vazao"]
    eficiencia_final = operacao["eficiencia"]
    anomalia = operacao["anomalia"]
    tipo_anomalia = operacao["tipo_anomalia"]
    status_operacional = operacao["status_operacional"]

    potencia_mw = calcular_potencia_mw(
        vazao_m3_s=vazao_final,
        head_m=usina["head_m"],
        eficiencia=eficiencia_final,
    )

    energia_incremental_mwh = potencia_mw * (SIMULATION_STEP_SECONDS / 3600.0)
    estado_usinas[usina_id]["energia_acumulada_mwh"] += energia_incremental_mwh

    temperatura = calcular_temperatura(
        temperatura_base=usina["temperatura_base_c"],
        eficiencia=eficiencia_final,
        anomalia=anomalia,
        tipo_anomalia=tipo_anomalia,
    )

    vibracao = calcular_vibracao(
        vibracao_base=usina["vibracao_base_mm_s"],
        anomalia=anomalia,
        tipo_anomalia=tipo_anomalia,
    )

    nivel_alerta = classificar_alerta(
        temperatura_c=temperatura,
        vibracao_mm_s=vibracao,
        eficiencia=eficiencia_final,
        anomalia=anomalia,
        manutencao=(status_operacional == "MANUTENCAO"),
    )

    fator_capacidade = 0.0
    potencia_max_teorica = calcular_potencia_mw(
        vazao_m3_s=usina["max_flow_m3_s"],
        head_m=usina["head_m"],
        eficiencia=usina["base_efficiency"],
    )
    if potencia_max_teorica > 0:
        fator_capacidade = potencia_mw / potencia_max_teorica

    evento = {
        "event_id": f"{usina_id}-{t}-{int(time.time() * 1000)}",
        "timestamp_utc": utc_now_iso(),
        "simulacao_t": t,

        "usina_id": usina_id,
        "usina_nome": usina["nome"],
        "rio": usina["rio"],

        "status_operacional": status_operacional,
        "nivel_alerta": nivel_alerta,
        "anomalia": anomalia,
        "tipo_anomalia": tipo_anomalia,

        "vazao_m3_s": round(vazao_final, 3),
        "vazao_bruta_m3_s": round(vazao, 3),
        "queda_bruta_m": round(usina["head_m"], 2),
        "eficiencia": round(eficiencia_final, 4),
        "potencia_mw": round(potencia_mw, 4),
        "fator_capacidade": round(fator_capacidade, 4),

        "chuva_mm_h": round(chuva, 3),
        "temperatura_turbina_c": temperatura,
        "vibracao_mm_s": vibracao,

        "energia_incremental_mwh": round(energia_incremental_mwh, 6),
        "energia_acumulada_mwh": round(estado_usinas[usina_id]["energia_acumulada_mwh"], 6),

        "densidade_agua_kg_m3": WATER_DENSITY,
        "gravidade_m_s2": GRAVITY,

        "metadata": {
            "fonte": "simulador_hidreletrico_kafka",
            "versao": "2.0",
            "topico": TOPIC,
        },
    }

    return evento


# =========================================================
# 📤 ENVIO
# =========================================================

def on_send_success(record_metadata):
    # deixei quieto pra não poluir o log
    pass


def on_send_error(excp):
    print(color(f"[ERRO KAFKA] {excp}", "31"), file=sys.stderr)


# =========================================================
# 🔁 LOOP PRINCIPAL
# =========================================================

def main() -> None:
    global message_count

    if EVENTS_PER_SECOND <= 0:
        raise ValueError("EVENTS_PER_SECOND precisa ser maior que zero.")

    intervalo = 1.0 / EVENTS_PER_SECOND
    t = 0

    print(color("Simulador hidrelétrico Kafka iniciado. Agora ficou decente.", "35"))
    print(color(f"Bootstrap: {BOOTSTRAP_SERVERS} | Topic: {TOPIC} | EPS: {EVENTS_PER_SECOND}", "35"))

    try:
        while running:
            inicio_ciclo = time.perf_counter()

            for usina in USINAS:
                evento = gerar_evento(usina, t)

                future = producer.send(
                    TOPIC,
                    key=evento["usina_id"],
                    value=evento,
                )
                future.add_callback(on_send_success)
                future.add_errback(on_send_error)

                log_event(evento)

                message_count += 1
                if message_count % FLUSH_EVERY_N_MESSAGES == 0:
                    producer.flush()

            t += SIMULATION_STEP_SECONDS

            duracao = time.perf_counter() - inicio_ciclo
            sleep_time = max(0.0, intervalo - duracao)
            time.sleep(sleep_time)

    except Exception as exc:
        print(color(f"[FALHA] {exc}", "31"), file=sys.stderr)
        raise
    finally:
        try:
            producer.flush()
            producer.close()
        except Exception:
            pass
        print(color("Simulador encerrado. Sem drama.", "35"))


if __name__ == "__main__":
    main()
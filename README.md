# Energy Generator Kafka + Spark Streaming

Simulador de geração hidrelétrica em tempo real usando **Python + Kafka**, preparado para evolução com **Spark Structured Streaming** e camadas de dados (*bronze/silver/gold*).

## Arquitetura atual

`Producer (Python) -> Kafka Topic -> Consumidores`

Atualmente o repositório inclui:

- **Producer**: simulador de telemetria de usinas hidrelétricas com anomalias e manutenção.
- **Kafka**: broker para ingestão em tempo real.

## Estrutura do repositório

```text
.
├── iot_hidro_eletric_simulator_.py
├── README.md
```

## Evento produzido

Cada mensagem enviada ao Kafka inclui, entre outros campos:

- identificação da usina (`usina_id`, `usina_nome`, `rio`)
- operação (`status_operacional`, `nivel_alerta`, `anomalia`, `tipo_anomalia`)
- métricas operacionais (`vazao_m3_s`, `eficiencia`, `potencia_mw`)
- condições de contexto (`chuva_mm_h`, `temperatura_turbina_c`, `vibracao_mm_s`)
- energia (`energia_incremental_mwh`, `energia_acumulada_mwh`)
- metadados (`metadata.fonte`, `metadata.versao`, `metadata.topico`)

## Pré-requisitos

- Python 3.10+
- Kafka acessível em `localhost:9092` (padrão atual do simulador)
- Dependência Python:

```bash
pip install kafka-python
```

## Como executar

1. Inicie o cluster Kafka e garanta que o broker esteja disponível em `localhost:9092`.
2. Rode o simulador:

```bash
python iot_hidro_eletric_simulator_.py
```

3. Interrompa com `Ctrl+C` para encerramento gracioso.

## Configurações principais

No arquivo `iot_hidro_eletric_simulator_.py`, você pode ajustar:

- `BOOTSTRAP_SERVERS`
- `TOPIC`
- `EVENTS_PER_SECOND`
- `ANOMALY_PROBABILITY`
- `MAINTENANCE_PROBABILITY`

## Próximo passo recomendado

Adicionar um consumer de **Spark Structured Streaming** para ler do tópico Kafka e persistir dados em **Parquet**, iniciando a camada **bronze** do lakehouse.


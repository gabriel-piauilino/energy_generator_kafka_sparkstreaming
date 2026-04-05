# 📋 PROJECT CONTEXT — Kafka Streaming Analytics (Hidrelétrica)

> Documento vivo. Atualizar sempre que houver mudança relevante de arquitetura,
> decisões técnicas, evolução de schema ou novos componentes.
>
> Última atualização: 2026-04-05

---

## 🎯 Objetivo do Projeto

Simular geração de energia hidrelétrica em tempo real, enviando eventos via **Kafka**
e processando com **Spark Structured Streaming**, seguindo uma arquitetura de dados
em camadas (Medallion Architecture: Bronze → Silver → Gold).

**Contexto de negócio:** pipeline próximo de um sistema real do setor elétrico brasileiro,
com dados de vazão, potência, eficiência e detecção de anomalias em usinas.

---

## 🏗️ Arquitetura Alvo

```
[Producer] → [Kafka] → [Spark Streaming] → [Bronze] → [Silver] → [Gold]
```

### Camadas

| Camada   | Responsabilidade                                                      |
|----------|-----------------------------------------------------------------------|
| Producer | Simula eventos de usinas hidrelétricas e publica no Kafka             |
| Kafka    | Ingestão e buffer de eventos em tempo real (tópico particionado)      |
| Bronze   | Raw: JSON preservado sem transformação, imutável, particionado por data |
| Silver   | Limpo, tipado, deduplicado, enriquecido com colunas derivadas          |
| Gold     | KPIs e agregações por janela temporal para dashboards e alertas        |

---

## 📁 Estrutura de Pastas (Alvo)

```
kafka_streaming_analytics/
│
├── producer/
│   ├── iot_hidro_eletric_simulator.py   # Simulador principal
│   ├── config.py                        # Constantes globais (BOOTSTRAP, TOPIC, EPS...)
│   └── schemas/
│       └── evento_hidreletrico_v1.json  # JSON Schema versionado dos eventos
│
├── consumer/
│   ├── jobs/
│   │   ├── bronze_ingest.py             # Kafka → Bronze
│   │   ├── silver_transform.py          # Bronze → Silver
│   │   └── gold_aggregations.py         # Silver → Gold
│   ├── schemas/
│   │   └── spark_schemas.py             # StructType PySpark dos eventos
│   └── utils/
│       ├── spark_session.py             # Factory do SparkSession
│       ├── kafka_utils.py               # Helpers de leitura Kafka
│       └── alert_rules.py              # Lógica de alertas e anomalias
│
├── lakehouse/
│   ├── bronze/                          # Dados raw particionados por event_date
│   ├── silver/                          # Dados limpos e enriquecidos
│   └── gold/                            # Agregações e KPIs
│
├── tests/
│   ├── unit/
│   │   ├── test_simulator_logic.py
│   │   └── test_silver_transforms.py
│   └── integration/
│       └── test_pipeline_e2e.py
│
├── monitoring/
│   └── metrics_exporter.py
│
├── docker/
│   └── docker-compose.yml               # Kafka + Zookeeper local
│
├── .github/workflows/
│   └── ci.yml                           # Lint + testes no push
│
├── requirements.txt
├── pyproject.toml                       # ruff + black + isort
├── PROJECT_CONTEXT.md                   # ← Este arquivo
└── README.md
```

---

## 🔄 Fluxo de Dados Ponta a Ponta

```
[producer/iot_hidro_eletric_simulator.py]
        │  JSON via KafkaProducer
        │  key = usina_id (garante ordem por usina)
        ▼
[Kafka Topic: energia-hidreletrica]
  • 4 partições (1 por usina)
  • retenção: 24h dev / 7 dias produção
        │
        ▼
[BRONZE — bronze_ingest.py]
  • readStream(format="kafka")
  • value → STRING, preservado sem transformação
  • Adiciona: kafka_offset, kafka_partition, kafka_timestamp
  • Escrita: Delta/Parquet particionado por event_date
  • Checkpoint: lakehouse/checkpoints/bronze/
        │
        ▼
[SILVER — silver_transform.py]
  • Aplica StructType via from_json (schema versionado)
  • Watermark 10min em timestamp_utc para deduplicação
  • Filtra: potencia_mw >= 0, campos obrigatórios não nulos
  • Colunas derivadas:
      - event_date, event_hour
      - is_anomaly_critical (anomalia AND nivel_alerta = CRITICO)
      - eficiencia_categoria (ALTA ≥0.88 / MEDIA / BAIXA <0.80)
      - potencia_pct_capacidade (fator_capacidade * 100)
  • Checkpoint: lakehouse/checkpoints/silver/
        │
        ▼
[GOLD — gold_aggregations.py]
  • Janelas temporais: tumbling 1min, 5min, 1h
  • groupBy(usina_id, window)
  • Escrita: Delta particionado por usina_id + data
```

---

## 📋 Schema dos Eventos Kafka (v2.0)

### Campos Obrigatórios

| Campo                    | Tipo              | Exemplo / Observação                        |
|--------------------------|-------------------|---------------------------------------------|
| `event_id`               | string            | `"hidro-01-120-1743800000123"`              |
| `timestamp_utc`          | string (ISO 8601) | Usado como watermark no Spark               |
| `simulacao_t`            | integer           | Passo de simulação                          |
| `usina_id`               | string (enum)     | Chave de particionamento Kafka              |
| `usina_nome`             | string            | —                                           |
| `rio`                    | string            | —                                           |
| `status_operacional`     | enum              | `OPERANDO / OPERANDO_COM_DESVIO / MANUTENCAO` |
| `nivel_alerta`           | enum              | `NORMAL / ATENCAO / CRITICO / INFO`         |
| `anomalia`               | boolean           | —                                           |
| `vazao_m3_s`             | float             | Vazão efetiva (m³/s)                        |
| `vazao_bruta_m3_s`       | float             | Vazão antes de anomalias                    |
| `queda_bruta_m`          | float             | Altura de queda (m)                         |
| `eficiencia`             | float [0,1]       | —                                           |
| `potencia_mw`            | float             | —                                           |
| `fator_capacidade`       | float [0,1]       | potencia / potencia_max_teorica             |
| `chuva_mm_h`             | float             | —                                           |
| `temperatura_turbina_c`  | float             | °C                                          |
| `vibracao_mm_s`          | float             | mm/s                                        |
| `energia_incremental_mwh`| float             | Energia gerada neste ciclo                  |
| `energia_acumulada_mwh`  | float             | Acumulado da usina desde o início           |
| `densidade_agua_kg_m3`   | float             | Constante: 1000.0                           |
| `gravidade_m_s2`         | float             | Constante: 10.81                            |
| `metadata.fonte`         | string            | `"simulador_hidreletrico_kafka"`            |
| `metadata.versao`        | string            | `"2.0"` — versionar ao evoluir o schema     |
| `metadata.topico`        | string            | `"energia-hidreletrica"`                    |

### Campos Opcionais (nullable)

| Campo           | Tipo         | Quando presente                                                                              |
|-----------------|--------------|----------------------------------------------------------------------------------------------|
| `tipo_anomalia` | string\|null | Apenas se `anomalia = true`: `CAVITACAO / AQUECIMENTO / VIBRACAO_ALTA / QUEDA_DE_RENDIMENTO / RESTRICAO_HIDRAULICA` |

---

## 🥇 Métricas Gold

| Métrica                | Janela | Descrição                              |
|------------------------|--------|----------------------------------------|
| `potencia_media_mw`    | 5min   | Saúde operacional contínua             |
| `potencia_max_mw`      | 5min   | Pico de geração                        |
| `eficiencia_media`     | 5min   | Benchmarking entre usinas              |
| `energia_gerada_mwh`   | 1h     | `SUM(energia_incremental_mwh)`         |
| `total_anomalias`      | 1h     | Contagem de eventos com `anomalia=true`|
| `tempo_em_manutencao_s`| 1h     | Segundos em `MANUTENCAO`               |
| `taxa_criticos_pct`    | 1h     | `COUNT(CRITICO) / COUNT(*) × 100`      |
| `temperatura_max_c`    | 1h     | Detecção de sobreaquecimento           |
| `vibracao_max_mm_s`    | 1h     | Detecção de desgaste mecânico          |
| `chuva_media_mm_h`     | 1h     | Correlação chuva × geração             |
| `vazao_media_m3_s`     | 1h     | Referência hidrológica                 |

---

## 🏭 Usinas Simuladas

| ID        | Nome               | Rio                 | Head (m) | Vazão Máx (m³/s) | Efic. Base |
|-----------|--------------------|---------------------|----------|------------------|------------|
| `hidro-01`| Usina Serra Azul   | Rio Grande          | 105      | 520              | 0.91       |
| `hidro-02`| Usina Vale Verde   | Rio Paranaíba       | 82       | 340              | 0.89       |
| `hidro-03`| Usina Pedra Clara  | Rio São Francisco   | 128      | 710              | 0.92       |
| `hidro-04`| Usina Horizonte    | Rio Tocantins       | 97       | 460              | 0.90       |

---

## ✅ Boas Práticas Adotadas

### Qualidade & Confiabilidade
- Checkpointing obrigatório em todos os jobs Spark (exactly-once)
- Watermark em `timestamp_utc` para late data (10min)
- `dropDuplicates(["event_id"])` com watermark na Silver

### Schema & Versionamento
- Schema versionado em `producer/schemas/evento_hidreletrico_v1.json`
- Campo `metadata.versao` em cada evento
- Regra: nunca remover campos obrigatórios — adicionar novos como opcionais primeiro

### Observabilidade
- Log de contagem de mensagens por micro-batch via `foreachBatch`
- Alertas quando `taxa_criticos_pct > 5%` ou `vibracao_max > 4.5 mm/s`
- Exportação de métricas (Prometheus/log JSON estruturado)

### Dev & CI
- Linter: **ruff** | Formatação: **black** | Imports: **isort**
- Testes unitários com **pytest**
- `docker-compose.yml` para Kafka+Zookeeper local
- GitHub Actions: lint + testes em todo push

---

## 🛤️ Roadmap / Próximos Passos

- [ ] Reorganizar estrutura de pastas (mover simulador para `producer/`)
- [ ] Criar `producer/config.py` com constantes extraídas
- [ ] Criar `consumer/schemas/spark_schemas.py` com StructType PySpark
- [ ] Implementar `bronze_ingest.py`
- [ ] Implementar `silver_transform.py`
- [ ] Implementar `gold_aggregations.py`
- [ ] Criar `docker-compose.yml` com Kafka + Zookeeper
- [ ] Adicionar `requirements.txt` e `pyproject.toml`
- [ ] Testes unitários do simulador
- [ ] CI com GitHub Actions
- [ ] (Futuro) Delta Lake no lugar de Parquet puro
- [ ] (Futuro) Schema Registry + Avro
- [ ] (Futuro) Orquestração com Prefect ou Airflow
- [ ] (Futuro) Dashboard com Spark SQL sobre tabelas Gold

---

## 📌 Decisões Técnicas Registradas

| Data       | Decisão                                                                 | Motivo                                              |
|------------|-------------------------------------------------------------------------|-----------------------------------------------------|
| 2026-04-05 | Usar JSON Schema versionado em arquivo, sem Schema Registry             | Reduzir overhead operacional no início              |
| 2026-04-05 | Persistência inicial com Parquet (Delta Lake como evolução futura)      | Simplicidade; Delta quando houver necessidade de MERGE/upsert |
| 2026-04-05 | Jobs Spark como processos independentes (sem orquestrador ainda)        | Evitar overengineering; orquestrar apenas com dependências reais |
| 2026-04-05 | 4 partições Kafka, 1 por usina, key = usina_id                          | Garantir ordem de eventos por usina                 |

---

## 🔗 Referências

- [PySpark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Kafka Python Client](https://kafka-python.readthedocs.io/)
- [Delta Lake](https://delta.io/)
- [Medallion Architecture (Databricks)](https://www.databricks.com/glossary/medallion-architecture)

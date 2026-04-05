#!/usr/bin/env pwsh
# =============================================================
# start.ps1 — Inicializa o stack completo do projeto
# Uso:
#   .\start.ps1           → sobe tudo
#   .\start.ps1 -Infra    → só Kafka + Kafka-UI (sem Spark e simulador)
#   .\start.ps1 -Down     → derruba tudo
#   .\start.ps1 -Logs     → tail de todos os logs
#   .\start.ps1 -Status   → status dos containers
# =============================================================

param(
    [switch]$Infra,
    [switch]$Down,
    [switch]$Logs,
    [switch]$Status,
    [switch]$Build
)

$REPO    = $PSScriptRoot                          # energy_generator_kafka_sparkstreaming/
$COMPOSE = "$REPO\docker\docker-compose.yml"

function Write-Header($msg) {
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host "  $msg" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
}

# ── Derrubar tudo ─────────────────────────────────────────────
if ($Down) {
    Write-Header "Derrubando stack..."
    docker compose -f $COMPOSE down -v --remove-orphans
    Write-Host "Stack encerrado." -ForegroundColor Yellow
    exit 0
}

# ── Status ────────────────────────────────────────────────────
if ($Status) {
    docker compose -f $COMPOSE ps
    exit 0
}

# ── Logs ──────────────────────────────────────────────────────
if ($Logs) {
    docker compose -f $COMPOSE logs -f --tail=100
    exit 0
}

# ── Verificar Docker ──────────────────────────────────────────
Write-Header "Verificando Docker..."
try {
    docker info | Out-Null
    Write-Host "Docker OK." -ForegroundColor Green
} catch {
    Write-Host "ERRO: Docker não está rodando. Inicie o Docker Desktop e tente novamente." -ForegroundColor Red
    exit 1
}

# ── Subir apenas infra Kafka ──────────────────────────────────
if ($Infra) {
    Write-Header "Subindo infraestrutura Kafka..."
    docker compose -f $COMPOSE up -d zookeeper kafka topic-init kafka-ui
    Write-Host ""
    Write-Host "Aguardando Kafka ficar saudável..." -ForegroundColor Yellow
    $retries = 0
    do {
        Start-Sleep -Seconds 5
        $health = docker inspect --format="{{.State.Health.Status}}" kafka 2>$null
        $retries++
        Write-Host "  [${retries}] Status kafka: $health"
    } while ($health -ne "healthy" -and $retries -lt 20)

    if ($health -eq "healthy") {
        Write-Host "Kafka pronto!" -ForegroundColor Green
    } else {
        Write-Host "Kafka demorou demais. Verifique com: .\start.ps1 -Logs" -ForegroundColor Red
    }
    Write-Host ""
    Write-Host "  Kafka-UI  → http://localhost:8080" -ForegroundColor Cyan
    exit 0
}

# ── Subir stack completo ──────────────────────────────────────
Write-Header "Subindo stack completo..."

$buildFlag = if ($Build) { "--build" } else { "" }

# 1. Infra primeiro
docker compose -f $COMPOSE up -d zookeeper kafka
Write-Host "Aguardando Kafka..." -ForegroundColor Yellow
$retries = 0
do {
    Start-Sleep -Seconds 6
    $health = docker inspect --format="{{.State.Health.Status}}" kafka 2>$null
    $retries++
    Write-Host "  [$retries] kafka: $health"
} while ($health -ne "healthy" -and $retries -lt 25)

if ($health -ne "healthy") {
    Write-Host "Kafka não ficou saudável. Abortando." -ForegroundColor Red
    exit 1
}

# 2. Criar tópico
docker compose -f $COMPOSE up topic-init
Write-Host "Tópico criado." -ForegroundColor Green

# 3. Kafka-UI
docker compose -f $COMPOSE up -d kafka-ui

# 4. Jobs Spark (em paralelo)
Write-Host "Subindo jobs Spark Bronze / Silver / Gold..." -ForegroundColor Yellow
if ($Build) {
    docker compose -f $COMPOSE up -d --build bronze silver gold
} else {
    docker compose -f $COMPOSE up -d bronze silver gold
}

# Pequena pausa para os jobs inicializarem
Start-Sleep -Seconds 8

# 5. Simulador
Write-Host "Subindo simulador Eletrobras..." -ForegroundColor Yellow
if ($Build) {
    docker compose -f $COMPOSE up -d --build simulator
} else {
    docker compose -f $COMPOSE up -d simulator
}

# 6. Dashboard
Write-Host "Subindo dashboard Streamlit..." -ForegroundColor Yellow
if ($Build) {
    docker compose -f $COMPOSE up -d --build dashboard
} else {
    docker compose -f $COMPOSE up -d dashboard
}

# ── Resumo ────────────────────────────────────────────────────
Write-Host ""
Write-Header "Stack rodando!"
docker compose -f $COMPOSE ps
Write-Host ""
Write-Host "  📊 Dashboard    → http://localhost:8501" -ForegroundColor Cyan
Write-Host "  🔎 Kafka-UI     → http://localhost:9091" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Logs em tempo real : .\start.ps1 -Logs"   -ForegroundColor DarkGray
Write-Host "  Encerrar tudo      : .\start.ps1 -Down"   -ForegroundColor DarkGray
Write-Host "  Rebuild completo   : .\start.ps1 -Build"  -ForegroundColor DarkGray

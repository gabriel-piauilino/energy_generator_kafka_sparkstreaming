"""
⚡ Real-Time Hydropower Monitoring Center
Spark Structured Streaming · Apache Kafka · Medallion Architecture
"""

import os, sys, time, json
from pathlib import Path
from datetime import datetime, timezone
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
import producer.config as cfg  # noqa

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_LAKEHOUSE = ROOT / "lakehouse"
GOLD_5MIN_PATH = BASE_LAKEHOUSE / "gold" / "window_5min"
GOLD_1H_PATH   = BASE_LAKEHOUSE / "gold" / "window_1h"
SILVER_PATH    = BASE_LAKEHOUSE / "silver"
CONFIG_FILE    = ROOT / "dashboard" / ".sim_config.json"

# ── Design tokens — VSCode Dark + Neon ───────────────────────────────────────
BG_BASE    = "#1e1e1e"   # fundo principal
BG_SURFACE = "#252526"   # cards / painéis
BG_RAISED  = "#2d2d30"   # elementos elevados
BORDER_CLR = "#3e3e42"   # bordas sutis

NEON_CYAN  = "#00d4ff"   # azul neon primário
NEON_BLUE  = "#569cd6"   # azul VSCode
NEON_GREEN = "#4ec9b0"   # verde neon / ok
NEON_WARN  = "#f7c948"   # amarelo neon / atenção
NEON_RED   = "#f14c4c"   # vermelho neon / crítico
NEON_PURP  = "#c586c0"   # lilás neon
NEON_ORNG  = "#ce9178"   # laranja VSCode

TEXT_PRI   = "#d4d4d4"   # texto principal
TEXT_SEC   = "#858585"   # texto secundário
TEXT_DIM   = "#4e4e4e"   # texto apagado

# Paleta de usinas — neons distintos
USINA_PALETTE = [NEON_CYAN, NEON_GREEN, NEON_BLUE, NEON_WARN, NEON_PURP]

PLOTLY_DARK = dict(
    paper_bgcolor=BG_SURFACE,
    plot_bgcolor=BG_BASE,
    font=dict(family="'Cascadia Code', 'JetBrains Mono', 'Fira Code', monospace", color=TEXT_PRI, size=11),
    xaxis=dict(gridcolor=BG_RAISED, linecolor=BORDER_CLR, tickfont=dict(color=TEXT_SEC)),
    yaxis=dict(gridcolor=BG_RAISED, linecolor=BORDER_CLR, tickfont=dict(color=TEXT_SEC)),
    legend=dict(bgcolor=BG_RAISED, bordercolor=BORDER_CLR, borderwidth=1, font=dict(color=TEXT_PRI)),
    margin=dict(l=12, r=12, t=40, b=12),
)

def plotly_layout(**extra):
    theme = {**PLOTLY_DARK}
    for k in list(extra):
        theme[k] = extra[k]
    return theme

CSS = f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

/* ── Reset global dark ─────────────────────────────────── */
html, body, [class*="css"], .stApp {{
    background-color: {BG_BASE} !important;
    color: {TEXT_PRI} !important;
    font-family: 'Inter', system-ui, sans-serif;
}}

/* ── Sidebar dark ────────────────────────────────────────── */
[data-testid="stSidebar"] {{
    background-color: {BG_SURFACE} !important;
    border-right: 1px solid {BORDER_CLR} !important;
}}
[data-testid="stSidebar"] * {{ color: {TEXT_PRI} !important; }}
[data-testid="stSidebar"] input,
[data-testid="stSidebar"] textarea,
[data-testid="stSidebar"] select {{
    background: {BG_RAISED} !important;
    color: {TEXT_PRI} !important;
    border: 1px solid {BORDER_CLR} !important;
    border-radius: 4px;
}}
[data-testid="stSidebar"] label {{ color: {TEXT_SEC} !important; font-size: 0.78rem !important; }}
[data-testid="stSidebar"] .stSlider [data-testid="stThumb"] {{ background: {NEON_CYAN} !important; }}
[data-testid="stSidebar"] [data-testid="stCheckbox"] span {{ border-color: {BORDER_CLR} !important; }}
[data-testid="stSidebar"] [data-testid="stCheckbox"] input:checked + span {{
    background: {NEON_CYAN} !important; border-color: {NEON_CYAN} !important;
}}
[data-testid="stSidebar"] .stButton button {{
    background: {NEON_CYAN} !important;
    color: {BG_BASE} !important;
    border: none !important;
    font-weight: 700;
    border-radius: 6px;
}}
[data-testid="stSidebar"] details summary {{
    color: {NEON_CYAN} !important;
    font-weight: 600;
}}

/* ── Main content ────────────────────────────────────────── */
.block-container {{ padding-top: 1rem !important; }}

/* ── Header ──────────────────────────────────────────────── */
.rt-header {{
    background: linear-gradient(135deg, {BG_RAISED} 0%, #1a2a3a 60%, #0a1a2a 100%);
    border: 1px solid {BORDER_CLR};
    border-left: 3px solid {NEON_CYAN};
    padding: 1.2rem 1.8rem;
    border-radius: 8px;
    margin-bottom: 1.2rem;
    display: flex; align-items: center; gap: 1rem;
}}
.rt-header .badge {{
    font-size: 0.7rem; padding: 2px 8px; border-radius: 20px;
    background: rgba(0,212,255,0.12); color: {NEON_CYAN};
    border: 1px solid rgba(0,212,255,0.3); margin-left: 8px;
    font-family: monospace; letter-spacing: 0.05em;
}}
.rt-header h1 {{ color: {TEXT_PRI}; margin: 0; font-size: 1.4rem; font-weight: 600; letter-spacing: -0.02em; }}
.rt-header h1 span {{ color: {NEON_CYAN}; }}
.rt-header p  {{ color: {TEXT_SEC}; margin: 4px 0 0; font-size: 0.78rem; font-family: monospace; }}

/* ── Section headers ─────────────────────────────────────── */
.sec-hdr {{
    font-size: 0.7rem; font-weight: 700; color: {NEON_CYAN};
    text-transform: uppercase; letter-spacing: 0.12em;
    border-bottom: 1px solid {BORDER_CLR};
    padding-bottom: 0.35rem; margin-bottom: 0.8rem;
    display: flex; align-items: center; gap: 6px;
}}

/* ── Pipeline bar ────────────────────────────────────────── */
.pipe-bar {{
    background: {BG_SURFACE};
    border: 1px solid {BORDER_CLR};
    border-radius: 6px;
    padding: 0.5rem 1rem;
    display: flex; gap: 0.8rem; align-items: center;
    margin-bottom: 1rem; font-size: 0.75rem;
    font-family: monospace; color: {TEXT_SEC};
    flex-wrap: wrap;
}}
.pipe-bar b {{ color: {TEXT_PRI}; }}
.pipe-bar .ok  {{ color: {NEON_GREEN}; font-weight: 700; }}
.pipe-bar .warn {{ color: {NEON_WARN}; font-weight: 700; }}
.pipe-bar .sep {{ color: {TEXT_DIM}; }}
.pipe-bar .ts  {{ margin-left: auto; color: {TEXT_DIM}; font-size: 0.7rem; }}

/* ── Metric cards ────────────────────────────────────────── */
[data-testid="metric-container"] {{
    background: {BG_SURFACE} !important;
    border: 1px solid {BORDER_CLR} !important;
    border-left: 3px solid {NEON_CYAN} !important;
    border-radius: 6px !important;
    padding: 0.7rem 0.9rem !important;
}}
[data-testid="metric-container"] label {{
    color: {TEXT_SEC} !important;
    font-size: 0.7rem !important;
    font-weight: 600 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.08em !important;
}}
[data-testid="metric-container"] [data-testid="stMetricValue"] {{
    color: {NEON_CYAN} !important; font-weight: 700 !important; font-size: 1.4rem !important;
}}
[data-testid="metric-container"] [data-testid="stMetricDelta"] {{
    font-size: 0.72rem !important;
}}

/* ── Tabs ─────────────────────────────────────────────────── */
[data-testid="stTabs"] {{
    background: {BG_SURFACE};
    border-radius: 6px;
    border: 1px solid {BORDER_CLR};
    padding: 0 0.5rem;
}}
[data-testid="stTabs"] button {{
    font-weight: 600 !important; font-size: 0.78rem !important;
    color: {TEXT_SEC} !important;
}}
[data-testid="stTabs"] button[aria-selected="true"] {{
    color: {NEON_CYAN} !important;
    border-bottom: 2px solid {NEON_CYAN} !important;
}}

/* ── Dataframe ───────────────────────────────────────────── */
[data-testid="stDataFrame"] {{
    background: {BG_SURFACE} !important;
    border: 1px solid {BORDER_CLR} !important;
    border-radius: 6px;
}}
.dvn-scroller {{ background: {BG_SURFACE} !important; }}

/* ── Divider ─────────────────────────────────────────────── */
hr {{ border-color: {BORDER_CLR} !important; margin: 1.2rem 0 !important; }}

/* ── Info/success/warning boxes ──────────────────────────── */
[data-testid="stAlert"] {{
    background: {BG_RAISED} !important;
    border: 1px solid {BORDER_CLR} !important;
    color: {TEXT_PRI} !important;
    border-radius: 6px !important;
}}

/* ── Expander ────────────────────────────────────────────── */
[data-testid="stExpander"] {{
    background: {BG_RAISED} !important;
    border: 1px solid {BORDER_CLR} !important;
    border-radius: 6px !important;
}}
[data-testid="stExpander"] summary {{ color: {NEON_CYAN} !important; font-weight: 600; }}

/* ── Toggle ──────────────────────────────────────────────── */
[data-testid="stToggle"] [data-testid="stWidgetLabel"] {{ color: {TEXT_SEC} !important; }}

/* ── Caption ─────────────────────────────────────────────── */
[data-testid="stCaptionContainer"], .stCaption {{ color: {TEXT_SEC} !important; }}

/* ── Scrollbar ───────────────────────────────────────────── */
::-webkit-scrollbar {{ width: 5px; height: 5px; }}
::-webkit-scrollbar-track {{ background: {BG_BASE}; }}
::-webkit-scrollbar-thumb {{ background: {BORDER_CLR}; border-radius: 3px; }}
::-webkit-scrollbar-thumb:hover {{ background: {TEXT_DIM}; }}
</style>
"""

# ── Helpers de dados ──────────────────────────────────────────────────────────

def _extract_partition_values(file_path: Path, root_path: Path) -> dict:
    parts = {}
    try:
        rel = file_path.relative_to(root_path)
        for part in rel.parts[:-1]:
            if "=" in part:
                col, val = part.split("=", 1)
                parts[col] = val
    except Exception:
        pass
    return parts


@st.cache_data(ttl=3)
def load_parquet_latest(path: Path, n_files: int = 60) -> pd.DataFrame:
    """Carrega os N arquivos Parquet mais recentes, injetando colunas de partição Hive."""
    if not path.exists():
        return pd.DataFrame()
    all_files = _safe_rglob_parquet(path)
    def safe_mtime(f):
        try: return f.stat().st_mtime
        except: return 0.0
    files = sorted(all_files, key=safe_mtime, reverse=True)
    if not files:
        return pd.DataFrame()
    frames = []
    for f in files[:n_files]:
        try:
            df = pd.read_parquet(f)
            for col, val in _extract_partition_values(f, path).items():
                if col not in df.columns:
                    df[col] = val
            frames.append(df)
        except Exception:
            pass
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


@st.cache_data(ttl=5)
def load_gold(path: Path) -> pd.DataFrame:
    df = load_parquet_latest(path, n_files=100)
    if df.empty:
        return df
    if "window_start" in df.columns:
        df["window_start"] = pd.to_datetime(df["window_start"], utc=True)
        df["window_end"]   = pd.to_datetime(df["window_end"],   utc=True)
        df = df.sort_values("window_start", ascending=False)
    return df


def _safe_rglob_parquet(path: Path):
    """rglob de *.parquet ignorando _temporary/_spark_metadata e arquivos sumidos."""
    result = []
    try:
        for f in path.rglob("*.parquet"):
            try:
                if "_temporary" not in f.parts and "_spark_metadata" not in f.parts:
                    result.append(f)
            except Exception:
                pass
    except Exception:
        pass
    return result


@st.cache_data(ttl=4)
def count_parquet_files(path: Path) -> int:
    if not path.exists(): return 0
    return len(_safe_rglob_parquet(path))


def _assign_usina_colors(usinas: list) -> dict:
    return {u: USINA_PALETTE[i % len(USINA_PALETTE)] for i, u in enumerate(sorted(usinas))}


def silver_as_gold_fallback(df_silver: pd.DataFrame) -> pd.DataFrame:
    required = {"event_ts", "potencia_mw", "usina_id", "eficiencia"}
    if df_silver.empty or not required.issubset(df_silver.columns):
        return pd.DataFrame()
    df = df_silver.copy()
    df["event_ts"] = pd.to_datetime(df["event_ts"], utc=True)
    recent = df[df["event_ts"] >= pd.Timestamp.utcnow() - pd.Timedelta(minutes=15)]
    if recent.empty: recent = df.sort_values("event_ts").tail(5000)
    grp = recent.groupby("usina_id")
    agg = grp.agg(potencia_media_mw=("potencia_mw","mean"), potencia_max_mw=("potencia_mw","max"),
                  eficiencia_media=("eficiencia","mean"), total_eventos=("potencia_mw","count")).reset_index()
    for col, src in [("oee_medio","oee"),("desgaste_medio","indice_desgaste"),
                     ("heat_rate_medio","heat_rate_kj_kwh"),("temperatura_max_c","temperatura_turbina_c"),
                     ("vibracao_max_mm_s","vibracao_mm_s"),("vazao_media_m3_s","vazao_m3_s"),
                     ("chuva_media_mm_h","chuva_mm_h"),("fator_capacidade_medio","fator_capacidade"),
                     ("energia_gerada_mwh","energia_incremental_mwh"),
                     ("eficiencia_relativa_media","eficiencia_relativa")]:
        agg[col] = grp[src].mean().values if src in recent.columns else 0.0
    for col, src in [("total_anomalias","anomalia"),("total_criticos","nivel_alerta"),("total_manutencao","status_operacional")]:
        agg[col] = (grp[src].sum().values if src == "anomalia" and src in recent.columns else 0)
    agg["score_saude"] = (0.5*agg["oee_medio"].clip(0,1)+0.5*agg["eficiencia_media"].clip(0,1)).round(4)
    agg["taxa_criticos_pct"] = 0.0
    agg["window_start"] = pd.Timestamp.utcnow()-pd.Timedelta(minutes=5)
    agg["window_end"]   = pd.Timestamp.utcnow()
    agg["janela_minutos"] = 5
    return agg


# ── Config ────────────────────────────────────────────────────────────────────

def salvar_config(config: dict):
    CONFIG_FILE.parent.mkdir(exist_ok=True)
    with open(CONFIG_FILE, "w") as f: json.dump(config, f, indent=2)

def carregar_config() -> dict:
    return json.load(open(CONFIG_FILE)) if CONFIG_FILE.exists() else {}


# ── Sidebar ───────────────────────────────────────────────────────────────────

def render_sidebar():
    st.sidebar.markdown(f"## ⚙️ Simulator Config")
    saved = carregar_config()

    with st.sidebar.expander("🔌 Kafka", expanded=False):
        bootstrap = st.text_input("Bootstrap Servers", value=saved.get("bootstrap_servers", cfg.BOOTSTRAP_SERVERS))
        topic     = st.text_input("Topic", value=saved.get("topic", cfg.TOPIC))
    with st.sidebar.expander("⚡ Produção", expanded=True):
        eps = st.slider("Eventos / segundo", 1, 500, saved.get("eps", cfg.EVENTS_PER_SECOND), 5)
        anomaly_prob = st.slider("P(Anomalia)", 0.0, 0.2, saved.get("anomaly_prob", cfg.ANOMALY_PROBABILITY), 0.005, format="%.3f")
        maint_prob   = st.slider("P(Manutenção)", 0.0, 0.05, saved.get("maint_prob", cfg.MAINTENANCE_PROBABILITY), 0.001, format="%.3f")
    with st.sidebar.expander("🏭 Usinas Ativas", expanded=True):
        usinas_disp = {u["id"]: u["nome"] for u in cfg.USINAS}
        default_ativas = saved.get("usinas_ativas", list(usinas_disp.keys()))
        usinas_ativas = [uid for uid, nome in usinas_disp.items()
                         if st.checkbox(nome, value=(uid in default_ativas), key=f"u_{uid}")]
    with st.sidebar.expander("🔄 Refresh", expanded=True):
        auto_refresh     = st.toggle("Auto-refresh", value=True)
        refresh_interval = st.slider("Intervalo (s)", 3, 30, 5) if auto_refresh else None
        rt_window        = st.slider("Janela tempo real (min)", 1, 60, 15)

    config = dict(bootstrap_servers=bootstrap, topic=topic, eps=eps,
                  anomaly_prob=anomaly_prob, maint_prob=maint_prob, usinas_ativas=usinas_ativas)
    if st.sidebar.button("💾 Salvar Config", use_container_width=True):
        salvar_config(config)
        st.sidebar.success("✅ Salvo!")

    st.sidebar.markdown("---")
    st.sidebar.caption(f"`{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`")
    return config, auto_refresh, refresh_interval, rt_window


# ── Componentes ───────────────────────────────────────────────────────────────

def render_header():
    st.markdown(f"""
    <div class="rt-header">
        <div style="font-size:2rem; line-height:1">⚡</div>
        <div>
            <h1>Real-Time <span>Hydropower</span> Monitoring Center
                <span class="badge">LIVE</span>
            </h1>
            <p>Spark Structured Streaming &nbsp;·&nbsp; Apache Kafka &nbsp;·&nbsp; Medallion Lakehouse &nbsp;·&nbsp; {datetime.now().strftime('%Y-%m-%d')}</p>
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_pipeline_status(n_silver, n_gold, n_silver_rec, n_gold_rec):
    s_cls  = "ok"   if n_silver > 0 else "warn"
    g_cls  = "ok"   if n_gold > 0   else "warn"
    s_icon = "●" if n_silver > 0 else "○"
    g_icon = "●" if n_gold > 0   else "○"
    st.markdown(f"""
    <div class="pipe-bar">
        <span><span class="ok">●</span> <b>Simulator</b></span>
        <span class="sep">→</span>
        <span><span class="ok">●</span> <b>Kafka</b></span>
        <span class="sep">→</span>
        <span><span class="{s_cls}">{s_icon}</span> <b>Bronze→Silver</b> {n_silver} files · <b>{n_silver_rec:,}</b> rows read</span>
        <span class="sep">→</span>
        <span><span class="{g_cls}">{g_icon}</span> <b>Gold</b> {n_gold} files · <b>{n_gold_rec:,}</b> aggs read</span>
        <span class="ts">updated {datetime.now().strftime('%H:%M:%S')}</span>
    </div>
    """, unsafe_allow_html=True)


def render_kpis(df_kpi: pd.DataFrame, fonte: str):
    st.markdown('<div class="sec-hdr">📊 KPIs — Parque Gerador</div>', unsafe_allow_html=True)
    if df_kpi.empty:
        st.info("⏳ Aguardando dados do pipeline...")
        return
    if fonte == "Silver":
        st.caption("⚠️ Estimativa Silver — Gold ainda processando")

    ult   = df_kpi.groupby("usina_id").first().reset_index()
    pot   = ult["potencia_media_mw"].sum()
    energ = df_kpi["energia_gerada_mwh"].sum() if "energia_gerada_mwh" in df_kpi.columns else 0
    oee   = ult["oee_medio"].mean()          if "oee_medio"          in ult.columns else 0
    saude = ult["score_saude"].mean()        if "score_saude"        in ult.columns else 0
    vaz   = ult["vazao_media_m3_s"].mean()   if "vazao_media_m3_s"   in ult.columns else 0
    anom  = int(ult["total_anomalias"].sum()) if "total_anomalias"   in ult.columns else 0
    crit  = int(ult["total_criticos"].sum())  if "total_criticos"    in ult.columns else 0

    c1,c2,c3,c4,c5,c6,c7 = st.columns(7)
    c1.metric("⚡ Potência Total",  f"{pot:,.0f} MW")
    c2.metric("🔋 Energia Gerada",  f"{energ:,.1f} MWh")
    c3.metric("📊 OEE Médio",       f"{oee:.1%}")
    c4.metric("🏥 Score Saúde",     f"{saude:.3f}")
    c5.metric("💧 Vazão Média",     f"{vaz:,.0f} m³/s")
    c6.metric("⚠️ Anomalias",      str(anom), delta="▲" if anom > 0 else None, delta_color="inverse")
    c7.metric("🔴 Críticos",        str(crit),  delta="▲" if crit > 0 else None, delta_color="inverse")


def render_timeseries_realtime(df_silver: pd.DataFrame, rt_window: int, colors: dict):
    """
    Timeseries de potência média por minuto por usina.
    Usa event_ts real dos dados — não depende de hora do sistema.
    Mostra os últimos rt_window minutos do dado mais recente.
    """
    st.markdown('<div class="sec-hdr">📈 Geração em Tempo Real</div>', unsafe_allow_html=True)

    if df_silver.empty or not {"event_ts","potencia_mw","usina_id"}.issubset(df_silver.columns):
        st.info("⏳ Aguardando dados Silver...")
        return

    df = df_silver.copy()
    df["event_ts"] = pd.to_datetime(df["event_ts"], utc=True)

    # Usa o timestamp máximo dos dados como âncora (não o relógio do sistema)
    # Isso garante que mesmo dados históricos aparecem na janela
    ts_max = df["event_ts"].max()
    cutoff = ts_max - pd.Timedelta(minutes=rt_window)
    df = df[df["event_ts"] >= cutoff]

    if df.empty:
        st.info(f"Sem dados no Silver. Silver reiniciando? Aguarde o próximo batch.")
        return

    # Agrega por minuto e usina
    df["minuto"] = df["event_ts"].dt.floor("1min")
    agg = df.groupby(["minuto","usina_id"]).agg(
        potencia_media=("potencia_mw","mean"),
        potencia_max=("potencia_mw","max"),
        n_eventos=("potencia_mw","count"),
    ).reset_index()

    fig = go.Figure()
    for usina in sorted(agg["usina_id"].unique()):
        color = colors.get(usina, NEON_CYAN)
        d = agg[agg["usina_id"]==usina].sort_values("minuto")
        if d.empty: continue

        # Converte cor hex para rgba para o fill
        try:
            r = int(color[1:3], 16)
            g = int(color[3:5], 16)
            b = int(color[5:7], 16)
            fill_color = f"rgba({r},{g},{b},0.10)"
        except Exception:
            fill_color = "rgba(0,212,255,0.10)"

        fig.add_trace(go.Scatter(
            x=d["minuto"], y=d["potencia_media"],
            name=usina.replace("_"," ").title(),
            mode="lines+markers",
            line=dict(color=color, width=2, shape="spline"),
            marker=dict(size=4, color=color,
                        line=dict(color=BG_BASE, width=1)),
            fill="tozeroy",
            fillcolor=fill_color,
            hovertemplate=(
                f"<b>{usina.replace('_',' ').title()}</b><br>"
                "%{x|%H:%M}<br>"
                "Potência média: <b>%{y:,.1f} MW</b><br>"
                "<extra></extra>"
            ),
        ))

    fig.update_layout(**plotly_layout(
        height=330,
        title=dict(
            text=f"Potência Média (MW) — últimos {rt_window} min  |  âncora: {ts_max.strftime('%H:%M:%S UTC')}",
            font=dict(size=12, color=TEXT_SEC), x=0,
        ),
        xaxis=dict(gridcolor=BG_RAISED, linecolor=BORDER_CLR, tickfont=dict(color=TEXT_SEC),
                   title=dict(text="Tempo (UTC)", font=dict(color=TEXT_SEC))),
        yaxis=dict(gridcolor=BG_RAISED, linecolor=BORDER_CLR, tickfont=dict(color=TEXT_SEC),
                   title=dict(text="MW", font=dict(color=TEXT_SEC))),
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                    bgcolor="rgba(0,0,0,0)", font=dict(color=TEXT_PRI, size=11)),
    ))
    st.plotly_chart(fig, use_container_width=True)

    st.caption(f"⏱ {len(df):,} eventos · {agg['usina_id'].nunique()} usinas · "
               f"janela: {cutoff.strftime('%H:%M')} → {ts_max.strftime('%H:%M')} UTC")


def render_gauges_row(df_kpi: pd.DataFrame, df_silver: pd.DataFrame, colors: dict):
    if df_kpi.empty and (df_silver is None or df_silver.empty): return
    src = df_kpi if not df_kpi.empty else silver_as_gold_fallback(df_silver)
    if src.empty: return

    ult  = src.groupby("usina_id").first().reset_index()
    cols = st.columns(len(ult))
    for i, (_, row) in enumerate(ult.iterrows()):
        fc    = float(row.get("fator_capacidade_medio") or 0) * 100
        oee   = float(row.get("oee_medio") or 0)
        score = float(row.get("score_saude") or 0)
        color = colors.get(row["usina_id"], NEON_CYAN)
        nome  = row["usina_id"].replace("_"," ").title()
        bar_c = NEON_RED if fc < 50 else NEON_WARN if fc < 75 else color

        with cols[i]:
            fig = go.Figure(go.Indicator(
                mode="gauge+number+delta",
                value=round(fc, 1),
                delta={"reference": 80, "valueformat": ".1f", "suffix": "%",
                       "increasing": {"color": NEON_GREEN},
                       "decreasing": {"color": NEON_RED}},
                number={"suffix": "%", "font": {"size": 20, "color": color}},
                title={"text": f"<b style='color:{TEXT_SEC}'>{nome}</b>",
                       "font": {"size": 10}},
                gauge={
                    "axis": {"range": [0, 100], "tickwidth": 1,
                             "tickcolor": BORDER_CLR,
                             "tickfont": {"color": TEXT_SEC, "size": 9}},
                    "bar": {"color": bar_c, "thickness": 0.28},
                    "bgcolor": BG_RAISED,
                    "borderwidth": 1,
                    "bordercolor": BORDER_CLR,
                    "steps": [
                        {"range": [0, 50],   "color": "rgba(241,76,76,0.08)"},
                        {"range": [50, 75],  "color": "rgba(247,201,72,0.08)"},
                        {"range": [75, 100], "color": "rgba(78,201,176,0.08)"},
                    ],
                    "threshold": {"line": {"color": TEXT_DIM, "width": 2},
                                  "thickness": 0.7, "value": 80},
                },
            ))
            fig.update_layout(
                paper_bgcolor=BG_SURFACE,
                plot_bgcolor=BG_SURFACE,
                font=dict(color=TEXT_PRI),
                height=185,
                margin=dict(l=8, r=8, t=30, b=8),
            )
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"`OEE {oee:.1%}` · `Saúde {score:.2f}`")


def render_operational_health(df_kpi: pd.DataFrame, df_silver: pd.DataFrame, colors: dict):
    st.markdown('<div class="sec-hdr">🏥 Saúde Operacional</div>', unsafe_allow_html=True)
    src = df_kpi if not df_kpi.empty else silver_as_gold_fallback(df_silver)
    if src.empty:
        st.info("Aguardando dados...")
        return

    ult        = src.groupby("usina_id").first().reset_index()
    usinas     = ult["usina_id"].tolist()
    color_list = [colors.get(u, NEON_CYAN) for u in usinas]
    nomes      = [u.replace("_"," ").title() for u in usinas]

    tab_oee, tab_wear, tab_heat, tab_hydro = st.tabs(
        ["OEE & Eficiência", "Desgaste & Temperatura", "Heat Rate & Vibração", "Hidrologia"])

    def _bar_chart(rows, cols_data, subtitles, hlines=None):
        fig = make_subplots(rows=1, cols=len(cols_data), subplot_titles=subtitles)
        for ci, (vals, clrs) in enumerate(cols_data, start=1):
            fig.add_trace(go.Bar(x=nomes, y=vals, marker_color=clrs,
                text=[f"{v:.2g}" for v in vals], textposition="outside",
                textfont=dict(color=TEXT_SEC, size=10)), row=1, col=ci)
        for h in (hlines or []):
            fig.add_hline(**h)
        fig.update_layout(**plotly_layout(height=280, showlegend=False))
        fig.update_annotations(font_color=TEXT_SEC)
        return fig

    with tab_oee:
        oee_v  = (ult["oee_medio"].fillna(0)*100).tolist()
        efic_v = (ult["eficiencia_media"].fillna(0)*100).tolist()
        fig = _bar_chart(1,
            [(oee_v, color_list), (efic_v, color_list)],
            ["OEE (%)", "Eficiência (%)"],
            hlines=[
                dict(y=70, line_dash="dot", line_color=NEON_RED, annotation_text="mín 70%", row=1, col=1),
                dict(y=88, line_dash="dot", line_color=NEON_GREEN, annotation_text="alvo 88%", row=1, col=2),
            ])
        fig.update_layout(yaxis_range=[0,108], yaxis2_range=[0,108])
        st.plotly_chart(fig, use_container_width=True)

    with tab_wear:
        desg = ult["desgaste_medio"].fillna(0).tolist()
        temp = ult["temperatura_max_c"].fillna(0).tolist()
        dc   = [NEON_RED if v>=0.65 else NEON_WARN if v>=0.35 else NEON_GREEN for v in desg]
        tc   = [NEON_RED if v>=80   else NEON_WARN if v>=72   else NEON_GREEN for v in temp]
        fig  = _bar_chart(1,
            [(desg, dc), (temp, tc)],
            ["Índice Desgaste", "Temperatura Máx (°C)"],
            hlines=[
                dict(y=0.65, line_dash="dot", line_color=NEON_RED, row=1, col=1),
                dict(y=80,   line_dash="dot", line_color=NEON_RED, row=1, col=2),
            ])
        st.plotly_chart(fig, use_container_width=True)

    with tab_heat:
        if "heat_rate_medio" in ult.columns:
            hr  = ult["heat_rate_medio"].fillna(0).tolist()
            vib = ult["vibracao_max_mm_s"].fillna(0).tolist() if "vibracao_max_mm_s" in ult.columns else [0]*len(nomes)
            ref = round(3600/0.93, 0)
            fig = _bar_chart(1,
                [(hr, color_list), (vib, color_list)],
                ["Heat Rate (kJ/kWh)", "Vibração Máx (mm/s)"],
                hlines=[
                    dict(y=ref, line_dash="dot", line_color=NEON_GREEN,
                         annotation_text=f"ref 93% ({ref:,.0f})", row=1, col=1),
                    dict(y=5.0, line_dash="dot", line_color=NEON_WARN,
                         annotation_text="alerta 5mm/s", row=1, col=2),
                ])
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Heat Rate disponível após Gold processar.")

    with tab_hydro:
        if "vazao_media_m3_s" in ult.columns:
            vaz  = ult["vazao_media_m3_s"].fillna(0).tolist()
            chuv = ult["chuva_media_mm_h"].fillna(0).tolist() if "chuva_media_mm_h" in ult.columns else [0]*len(nomes)
            fig  = _bar_chart(1,
                [(vaz, color_list), (chuv, [NEON_BLUE]*len(nomes))],
                ["Vazão Média (m³/s)", "Chuva Média (mm/h)"])
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Dados hidrológicos disponíveis após Gold processar.")


def render_alerts_panel(df_gold: pd.DataFrame, df_silver: pd.DataFrame, colors: dict):
    st.markdown('<div class="sec-hdr">🚨 Alertas & Anomalias</div>', unsafe_allow_html=True)
    col_dist, col_table = st.columns([1, 2])

    with col_dist:
        st.caption("Contagem por usina · janela 5min (Gold)")
        if not df_gold.empty and "total_anomalias" in df_gold.columns:
            ult   = df_gold.groupby("usina_id").first().reset_index()
            nomes = [u.replace("_"," ").title() for u in ult["usina_id"]]
            fig   = go.Figure()
            if "total_anomalias"  in ult.columns:
                fig.add_trace(go.Bar(name="Anomalias",  x=nomes, y=ult["total_anomalias"],
                                     marker_color=NEON_WARN,  opacity=0.9))
            if "total_criticos"   in ult.columns:
                fig.add_trace(go.Bar(name="Críticos",   x=nomes, y=ult["total_criticos"],
                                     marker_color=NEON_RED,   opacity=0.9))
            if "total_manutencao" in ult.columns:
                fig.add_trace(go.Bar(name="Manutenção", x=nomes, y=ult["total_manutencao"],
                                     marker_color=NEON_PURP,  opacity=0.9))
            fig.update_layout(**plotly_layout(
                height=240, barmode="group", showlegend=True,
                legend=dict(orientation="h", y=1.1, font=dict(size=10, color=TEXT_PRI)),
            ))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Aguardando Gold...")

    with col_table:
        st.caption("Últimas anomalias detectadas · Silver")
        COLS = ["event_ts","usina_id","tipo_anomalia","nivel_alerta",
                "potencia_mw","eficiencia","temperatura_turbina_c","vibracao_mm_s"]
        if not df_silver.empty and "anomalia" in df_silver.columns:
            anom = df_silver[df_silver["anomalia"]==True].copy()
            if not anom.empty:
                anom  = anom.sort_values("event_ts", ascending=False).head(25)
                exist = [c for c in COLS if c in anom.columns]
                ren   = {"event_ts":"Time","usina_id":"Usina","tipo_anomalia":"Tipo",
                         "nivel_alerta":"Level","potencia_mw":"MW","eficiencia":"η",
                         "temperatura_turbina_c":"°C","vibracao_mm_s":"Vib"}
                disp  = anom[exist].rename(columns=ren).copy()
                if "Time" in disp.columns:
                    disp["Time"] = pd.to_datetime(disp["Time"]).dt.strftime("%H:%M:%S")

                def _style(val):
                    return {
                        "CRITICO": f"background:{NEON_RED}22;color:{NEON_RED};font-weight:700",
                        "ATENCAO": f"background:{NEON_WARN}22;color:{NEON_WARN};font-weight:600",
                        "NORMAL":  f"color:{NEON_GREEN}",
                    }.get(str(val), "")

                try:
                    styled = disp.style.map(_style, subset=["Level"]) if "Level" in disp.columns else disp.style
                    st.dataframe(styled, use_container_width=True, height=250)
                except Exception:
                    st.dataframe(disp, use_container_width=True, height=250)
            else:
                st.success("✅ Nenhuma anomalia recente detectada.")
        else:
            st.info("Aguardando dados Silver...")


def render_correlation(df_kpi: pd.DataFrame, colors: dict):
    st.markdown('<div class="sec-hdr">🌧️ Análise Hidrológica</div>', unsafe_allow_html=True)
    col_sc, col_box = st.columns(2)

    with col_sc:
        st.caption("Chuva × Potência")
        if not df_kpi.empty and "chuva_media_mm_h" in df_kpi.columns:
            dp = df_kpi.dropna(subset=["chuva_media_mm_h","potencia_media_mw"]).copy()
            if not dp.empty:
                fig = px.scatter(dp, x="chuva_media_mm_h", y="potencia_media_mw",
                    color="usina_id", color_discrete_map=colors,
                    labels={"chuva_media_mm_h":"Chuva (mm/h)","potencia_media_mw":"Potência (MW)","usina_id":""},
                    hover_data=[c for c in ["window_start","eficiencia_media"] if c in dp.columns])
                x, y = dp["chuva_media_mm_h"].values, dp["potencia_media_mw"].values
                if len(x) >= 2:
                    try:
                        c = np.polyfit(x, y, 1)
                        xl = np.linspace(x.min(), x.max(), 50)
                        fig.add_scatter(x=xl, y=np.polyval(c, xl), mode="lines",
                            line=dict(color=TEXT_DIM, dash="dash", width=1.5), name="trend")
                    except: pass
                fig.update_layout(**plotly_layout(height=270, showlegend=True))
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Sem dados suficientes.")
        else:
            st.info("Aguardando Gold com dados de chuva...")

    with col_box:
        st.caption("Distribuição de Potência por Usina")
        sv = load_parquet_latest(SILVER_PATH, n_files=80)
        if not sv.empty and "potencia_mw" in sv.columns and "usina_id" in sv.columns:
            fig = px.box(sv, x="usina_id", y="potencia_mw",
                color="usina_id", color_discrete_map=colors,
                labels={"usina_id":"Usina","potencia_mw":"Potência (MW)"},
                points="outliers")
            fig.update_traces(marker=dict(size=3, opacity=0.6))
            fig.update_layout(**plotly_layout(height=270, showlegend=False))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Aguardando Silver...")


def render_gold_table(df5: pd.DataFrame, df1h: pd.DataFrame):
    st.markdown('<div class="sec-hdr">📋 Agregações Gold</div>', unsafe_allow_html=True)
    tab5, tab1h = st.tabs(["⏱ 5 minutos", "🕐 1 hora"])
    COLS = ["window_start","usina_id","total_eventos","potencia_media_mw","potencia_max_mw",
            "eficiencia_media","oee_medio","heat_rate_medio","desgaste_medio",
            "energia_gerada_mwh","total_anomalias","taxa_criticos_pct","score_saude",
            "temperatura_max_c","vibracao_max_mm_s","vazao_media_m3_s","chuva_media_mm_h"]

    def _show(df):
        if df.empty: st.info("Sem dados ainda."); return
        exist = [c for c in COLS if c in df.columns]
        disp  = df[exist].head(200).copy()
        for c in disp.select_dtypes("float").columns:
            disp[c] = disp[c].round(4)
        st.dataframe(disp, use_container_width=True, height=380,
            column_config={
                "score_saude":      st.column_config.ProgressColumn("Score Saúde",  min_value=0, max_value=1),
                "oee_medio":        st.column_config.ProgressColumn("OEE",          min_value=0, max_value=1),
                "taxa_criticos_pct":st.column_config.NumberColumn("Críticos %",     format="%.2f%%"),
            })

    with tab5:  _show(df5)
    with tab1h: _show(df1h)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    st.set_page_config(
        page_title="⚡ Hydropower Monitoring Center",
        page_icon="⚡",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.markdown(CSS, unsafe_allow_html=True)

    config, auto_refresh, refresh_interval, rt_window = render_sidebar()
    render_header()

    # Carrega dados
    df_gold_5min = load_gold(GOLD_5MIN_PATH)
    df_gold_1h   = load_gold(GOLD_1H_PATH)
    df_silver    = load_parquet_latest(SILVER_PATH, n_files=80)

    # Cores dinâmicas
    all_usinas = []
    if not df_silver.empty and "usina_id" in df_silver.columns:
        all_usinas = sorted(df_silver["usina_id"].dropna().unique().tolist())
    elif not df_gold_5min.empty and "usina_id" in df_gold_5min.columns:
        all_usinas = sorted(df_gold_5min["usina_id"].dropna().unique().tolist())
    colors = _assign_usina_colors(all_usinas)

    # Pipeline status
    n_s = count_parquet_files(SILVER_PATH)
    n_g = count_parquet_files(GOLD_5MIN_PATH)
    render_pipeline_status(n_s, n_g, len(df_silver), len(df_gold_5min))

    # Fonte KPI
    if not df_gold_5min.empty:
        df_kpi, fonte = df_gold_5min, "Gold"
    elif not df_silver.empty:
        df_kpi, fonte = silver_as_gold_fallback(df_silver), "Silver"
    else:
        df_kpi, fonte = pd.DataFrame(), "—"

    render_kpis(df_kpi, fonte)
    st.divider()

    col_ts, col_g = st.columns([3, 2])
    with col_ts:
        render_timeseries_realtime(df_silver, rt_window, colors)
    with col_g:
        st.markdown('<div class="sec-hdr">🎯 Fator de Capacidade</div>', unsafe_allow_html=True)
        render_gauges_row(df_kpi, df_silver, colors)

    st.divider()
    render_operational_health(df_kpi, df_silver, colors)

    st.divider()
    render_alerts_panel(df_gold_5min, df_silver, colors)

    st.divider()
    render_correlation(df_kpi, colors)

    st.divider()
    render_gold_table(df_gold_5min, df_gold_1h)

    if auto_refresh and refresh_interval:
        time.sleep(refresh_interval)
        st.rerun()


if __name__ == "__main__":
    main()

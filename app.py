import streamlit as st
import pandas as pd
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from src.analytics.betting_insights import BettingAnalyzer
from src.analytics.value_bet_finder import ValueBetFinder
from src.models.feature_engineer import FeatureEngineer
from src.models.match_predictor import MatchPredictor

# ──────────────────────────────────────────────
# Configuração de UI
# ──────────────────────────────────────────────
st.set_page_config(
    page_title="CBLOL Betting Predictor",
    page_icon="🏆",
    layout="wide",
)

st.title("🏆 CBLOL Analytics & Betting Insights")
st.caption("Análise de Value Bets para o CBLOL 2026 com Machine Learning")
st.markdown("---")

# ──────────────────────────────────────────────
# Carregamento de Dados
# ──────────────────────────────────────────────
GOLD_PATH = "data/gold/cblol_2026_Split 1_gold.csv"
MODEL_PATH = "data/models/predictor.pkl"

if not os.path.exists(GOLD_PATH):
    st.error(f"Arquivo Gold nao encontrado em `{GOLD_PATH}`. Rode o `main.py` primeiro!")
    st.stop()

df = pd.read_csv(GOLD_PATH)
analyzer = BettingAnalyzer()
team_stats = analyzer.get_team_stats(df)
all_teams = sorted(team_stats.index.tolist())

# ──────────────────────────────────────────────
# Carrega o Modelo ML
# ──────────────────────────────────────────────
@st.cache_resource
def load_model():
    if os.path.exists(MODEL_PATH):
        return MatchPredictor.load()
    return None

predictor = load_model()
model_available = predictor is not None

# ──────────────────────────────────────────────
# SIDEBAR
# ──────────────────────────────────────────────
st.sidebar.header("Configurar Confronto")

team_a = st.sidebar.selectbox("Time A (Blue Side)", all_teams, index=0)
team_b = st.sidebar.selectbox("Time B (Red Side)", all_teams, index=min(1, len(all_teams)-1))

st.sidebar.markdown("---")
st.sidebar.subheader("Odds da Casa de Apostas")
st.sidebar.caption("Informe as odds decimais (ex: 1.75) que a casa está oferecendo.")

odd_a = st.sidebar.number_input(f"Odd para {team_a} vencer", min_value=1.01, max_value=50.0, value=1.80, step=0.05)
odd_b = st.sidebar.number_input(f"Odd para {team_b} vencer", min_value=1.01, max_value=50.0, value=2.10, step=0.05)

st.sidebar.markdown("---")
st.sidebar.caption("🤖 Modelo: XGBoost | Dados: Oracle's Elixir + Leaguepedia")

# ──────────────────────────────────────────────
# VALIDAÇÃO: não permitir o mesmo time
# ──────────────────────────────────────────────
if team_a == team_b:
    st.warning("Selecione dois times diferentes na sidebar.")
    st.stop()

# ──────────────────────────────────────────────
# SEÇÃO 1: Predição com ML + Value Bet
# ──────────────────────────────────────────────
st.header(f"🥊 {team_a}  vs  {team_b}")

if model_available:
    # Busca as últimas stats de cada time para alimentar o modelo
    engineer = FeatureEngineer(window=5)
    X, y, meta, feature_cols = engineer.build_training_set(df)

    def get_last_stats(team_name):
        mask = meta["teamname"] == team_name
        if not mask.any():
            return None
        last_idx = meta[mask].index[-1]
        return X.loc[last_idx].drop("side_blue", errors="ignore").to_dict()

    stats_a = get_last_stats(team_a)
    stats_b = get_last_stats(team_b)

    if stats_a is None or stats_b is None:
        st.warning("Um dos times nao tem historico suficiente para predicao pelo modelo.")
        model_proba_a = 0.5
    else:
        resultado = predictor.predict_proba_matchup(stats_a, stats_b, team_a_side="Blue")
        model_proba_a = resultado["team_a"]

    model_proba_b = 1 - model_proba_a

    # ── Value Bet Analysis ──
    finder = ValueBetFinder(min_value_threshold=0.03)
    result_a, result_b = finder.analyze(team_a, team_b, model_proba_a, odd_a, odd_b)
    vig = finder.get_vig(odd_a, odd_b)

    # ── Métricas de predição ──
    col_pred1, col_pred2, col_pred3 = st.columns(3)

    with col_pred1:
        st.metric(
            label=f"Prob. Modelo — {team_a}",
            value=f"{model_proba_a*100:.1f}%",
            delta=f"{(model_proba_a - 1/odd_a)*100:+.1f}% vs odd",
        )
        fair_odd_a = round(1 / model_proba_a, 2) if model_proba_a > 0 else "-"
        st.caption(f"Odd justa estimada: **{fair_odd_a}** | Casa oferece: **{odd_a}**")

    with col_pred2:
        st.metric(
            label=f"Prob. Modelo — {team_b}",
            value=f"{model_proba_b*100:.1f}%",
            delta=f"{(model_proba_b - 1/odd_b)*100:+.1f}% vs odd",
        )
        fair_odd_b = round(1 / model_proba_b, 2) if model_proba_b > 0 else "-"
        st.caption(f"Odd justa estimada: **{fair_odd_b}** | Casa oferece: **{odd_b}**")

    with col_pred3:
        st.metric("Margem da Casa (Vig)", f"{vig*100:.1f}%")
        st.caption("Vig < 5% = casa competitiva | > 10% = casa abusiva")

    st.markdown("---")

    # ── Recomendação de Value Bet ──
    st.subheader("💡 Análise de Value Bet")

    for result in [result_a, result_b]:
        ev_pct = result.value * 100
        edge_pct = result.edge * 100
        kelly_pct = max(result.kelly_fraction * 100, 0)

        if result.is_value_bet:
            if ev_pct >= 15:
                level = "🔥 VALUE BET FORTE"
                color = "success"
            elif ev_pct >= 8:
                level = "✅ Value Bet Moderado"
                color = "success"
            else:
                level = "🟡 Value Bet Fraco"
                color = "warning"

            msg = (
                f"**{level} — {result.team}** | "
                f"EV: {ev_pct:+.1f}% | "
                f"Edge: {edge_pct:+.1f}% | "
                f"Kelly Rec.: {kelly_pct:.1f}% da banca"
            )
            if color == "success":
                st.success(msg)
            else:
                st.warning(msg)
        else:
            st.info(
                f"**{result.team}** — {result.recommendation} "
                f"(EV: {ev_pct:+.1f}%)"
            )

    st.markdown("---")

    # ── Tabela detalhada ──
    with st.expander("Ver tabela detalhada de analise"):
        summary = finder.summarize(result_a, result_b)
        st.dataframe(summary)

else:
    st.warning("Modelo ML nao encontrado. Rode `python src/models/model_trainer.py` para gerar o modelo.")

# ──────────────────────────────────────────────
# SEÇÃO 2: Estatísticas por Time (H2H)
# ──────────────────────────────────────────────
st.header("📊 Comparativo Head-to-Head")

col1, col2 = st.columns(2)

stats_display = ["win_rate", "fb_rate", "inibi_rate", "avg_duration", "avg_k_diff"]
labels = {
    "win_rate": "Win Rate (%)",
    "fb_rate": "First Blood Rate (%)",
    "inibi_rate": "First Inhibitor Rate (%)",
    "avg_duration": "Duração Média (min)",
    "avg_k_diff": "Kill Diff. Médio",
}

for col, team in zip([col1, col2], [team_a, team_b]):
    with col:
        st.subheader(team)
        if team in team_stats.index:
            row = team_stats.loc[team]
            for stat in stats_display:
                if stat in row:
                    val = row[stat]
                    label = labels.get(stat, stat)
                    if stat == "avg_duration":
                        st.metric(label, f"{val:.1f} min")
                    elif stat == "avg_k_diff":
                        st.metric(label, f"{val:+.1f}")
                    else:
                        st.metric(label, f"{val:.1f}%")
        else:
            st.write("Dados nao disponiveis.")

st.markdown("---")

# ──────────────────────────────────────────────
# SEÇÃO 3: Tabela Geral do Split
# ──────────────────────────────────────────────
st.subheader("🏅 Power Ranking do Split")
display_cols = [c for c in ["jogos", "win_rate", "fb_rate", "inibi_rate", "avg_duration", "avg_k_diff"] if c in team_stats.columns]
styled = team_stats[display_cols].sort_values("win_rate", ascending=False)
st.dataframe(
    styled.style.background_gradient(cmap="Greens", subset=[c for c in ["win_rate", "fb_rate", "inibi_rate"] if c in styled.columns]),
    use_container_width=True,
)

# ──────────────────────────────────────────────
# SEÇÃO 4: Análise de Duração (Over/Under)
# ──────────────────────────────────────────────
st.subheader("⏰ Histórico de Duração das Partidas (Over/Under)")
team_games = df[df["position"] == "team"]
if "gamelength" in team_games.columns:
    duration_data = (
        team_games.groupby("gameid")["gamelength"]
        .first()
        .reset_index()
        .rename(columns={"gamelength": "Duracao (min)"})
        .set_index("gameid")
    )
    avg_duration = duration_data["Duracao (min)"].mean()
    st.caption(f"Duração média do Split: **{avg_duration:.1f} minutos**")
    st.line_chart(duration_data)
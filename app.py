import streamlit as st
import pandas as pd
import os
from src.analytics.betting_insights import BettingAnalyzer

# Configurações de UI
st.set_page_config(page_title="CBLOL Betting Predictor", layout="wide")

st.title("🏆 CBLOL Analytics & Betting Insights")
st.markdown("---")

# --- CARREGAMENTO DE DADOS ---
GOLD_PATH = "data/gold/cblol_2026_Split 1_gold.csv"

if not os.path.exists(GOLD_PATH):
    st.error(f"Arquivo Gold não encontrado em {GOLD_PATH}. Rode o `main.py` primeiro!")
else:
    df = pd.read_csv(GOLD_PATH)
    analyzer = BettingAnalyzer()
    
    # --- SIDEBAR / FILTROS ---
    st.sidebar.header("Configurações de Análise")
    all_teams = sorted(df['teamname'].unique())
    
    # Seleção de Confronto
    st.sidebar.subheader("Simular Confronto")
    team_a = st.sidebar.selectbox("Time A (Blue)", all_teams, index=0)
    team_b = st.sidebar.selectbox("Time B (Red)", all_teams, index=1)
    
    # --- DASHBOARD PRINCIPAL ---
    team_stats = analyzer.get_team_stats(df)
    
    # Seção 1: Comparativo Direto (Head-to-Head)
    st.header(f"🥊 {team_a} vs {team_b}")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Win Rate", f"{team_stats.loc[team_a, 'win_rate']}%", 
                  f"{team_stats.loc[team_a, 'win_rate'] - team_stats.loc[team_b, 'win_rate']:.1f}% vs Opp")
        st.metric("First Blood %", f"{team_stats.loc[team_a, 'fb_rate']}%")

    with col2:
        st.metric("Média Duração", f"{team_stats.loc[team_a, 'avg_duration']:.1f} min")
        st.metric("Avg Kill Margin (Handicap)", f"{team_stats.loc[team_a, 'avg_k_diff']}")

    with col3:
        st.metric("First Inhibitor %", f"{team_stats.loc[team_a, 'inibi_rate']}%")
        st.write("**Sugestão de Aposta:**")
        # Lógica simples de exemplo para assertividade
        if team_stats.loc[team_a, 'fb_rate'] > 70:
            st.success(f"Forte tendência FB para {team_a}")
        elif team_stats.loc[team_b, 'fb_rate'] > 70:
            st.success(f"Forte tendência FB para {team_b}")
        else:
            st.warning("Mercado de FB equilibrado")

    st.markdown("---")

    # Seção 2: Tabela Geral de Power Ranking
    st.subheader("📊 Estatísticas Gerais do Split")
    st.dataframe(team_stats.style.background_gradient(cmap='Greens', subset=['win_rate', 'fb_rate', 'inibi_rate']))

    # Seção 3: Duração de Partida (Over/Under)
    st.subheader("⏰ Análise de Tempo (Over/Under)")
    st.line_chart(df[df['position'] == 'team'].groupby('gameid')['gamelength'].mean())
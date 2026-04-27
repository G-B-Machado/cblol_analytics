"""
Script de treino do modelo preditivo do CBLOL Analytics.

Uso:
    python src/models/model_trainer.py

O script:
1. Carrega o Gold CSV
2. Engenharia de features (rolling window + H2H)
3. Treina o modelo (XGBoost ou Logistic Regression)
4. Avalia métricas (AUC-ROC, Accuracy) com Cross-Validation
5. Salva o modelo em data/models/
6. Demonstra uma predição de exemplo
"""

import os
import sys
import pandas as pd
import numpy as np
from sklearn.model_selection import cross_val_score, StratifiedKFold
from sklearn.metrics import roc_auc_score, accuracy_score

# Garante que o projeto está no path ao rodar diretamente
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from src.models.feature_engineer import FeatureEngineer
from src.models.match_predictor import MatchPredictor

GOLD_PATH = "data/gold/cblol_2026_Split 1_gold.csv"
WINDOW = 5


def avaliar_modelo(model, X, y, feature_cols):
    """Avalia o modelo com Cross-Validation estratificado."""
    print("\n" + "=" * 40)
    print("[AVALIACAO DO MODELO] Cross-Validation 5-Fold")
    print("=" * 40)

    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

    # AUC-ROC
    auc_scores = cross_val_score(model.model, X[feature_cols], y, cv=cv, scoring="roc_auc")
    print(f"\n  AUC-ROC  -> Media: {auc_scores.mean():.4f}  |  Std: {auc_scores.std():.4f}")
    print(f"  (Scores por fold: {[f'{s:.3f}' for s in auc_scores]})")

    # Accuracy
    acc_scores = cross_val_score(model.model, X[feature_cols], y, cv=cv, scoring="accuracy")
    print(f"  Accuracy -> Media: {acc_scores.mean():.4f}  |  Std: {acc_scores.std():.4f}")

    # Interpretação
    mean_auc = auc_scores.mean()
    if mean_auc >= 0.70:
        print("\n  [FORTE] AUC acima de 0.70 - confiavel para Value Betting.")
    elif mean_auc >= 0.60:
        print("\n  [RAZOAVEL] AUC entre 0.60-0.70. Util, mas use com cautela.")
    else:
        print("\n  [FRACO] AUC abaixo de 0.60. Precisa de mais dados ou features.")

    print("=" * 40)
    return auc_scores.mean()


def demonstrar_predicao(model, X, meta, feature_cols):
    """Mostra uma predição de exemplo usando as médias históricas dos times."""
    times = meta["teamname"].unique()
    if len(times) < 2:
        print("\n[!] Dados insuficientes para demonstração de predição.")
        return

    time_a = times[0]
    time_b = times[1]

    # Pega as últimas stats conhecidas de cada time
    def get_last_stats(team_name):
        mask = meta["teamname"] == team_name
        last_idx = meta[mask].index[-1]
        return X.loc[last_idx].drop("side_blue", errors="ignore").to_dict()

    stats_a = get_last_stats(time_a)
    stats_b = get_last_stats(time_b)

    resultado = model.predict_proba_matchup(stats_a, stats_b, team_a_side="Blue")

    print(f"\n[PREDICAO DE EXEMPLO] {time_a} (Blue) vs {time_b} (Red)")
    print(f"   Chance de vitoria {time_a}: {resultado['team_a']*100:.1f}%")
    print(f"   Chance de vitoria {time_b}: {resultado['team_b']*100:.1f}%")

    # Converte probabilidade em odd justa (sem margem da casa)
    odd_a = round(1 / resultado["team_a"], 2) if resultado["team_a"] > 0 else "INF"
    odd_b = round(1 / resultado["team_b"], 2) if resultado["team_b"] > 0 else "INF"
    print(f"\n   [ODD JUSTA] {time_a}: {odd_a}  |  {time_b}: {odd_b}")
    print("   (Se a casa oferecer odds ACIMA desses valores, pode ser uma Value Bet!)")


def main():
    # 1. Carregar dados
    if not os.path.exists(GOLD_PATH):
        print(f"[ERRO] Gold não encontrado em '{GOLD_PATH}'. Rode o main.py primeiro.")
        sys.exit(1)

    print(f"[*] Carregando dados de '{GOLD_PATH}'...")
    df = pd.read_csv(GOLD_PATH)
    print(f"[+] {len(df)} linhas carregadas.")

    # 2. Engenharia de features
    engineer = FeatureEngineer(window=WINDOW)
    X, y, meta, feature_cols = engineer.build_training_set(df)

    print(f"\n[+] Features utilizadas: {feature_cols}")
    print(f"[+] Amostras de treino:  {len(X)}")
    print(f"[+] Distribuição do target: {y.value_counts().to_dict()}")

    # 3. Treinar modelo
    predictor = MatchPredictor(use_xgboost=True)
    predictor.train(X, y, feature_cols)

    # 4. Avaliar
    avaliar_modelo(predictor, X, y, feature_cols)

    # 5. Salvar
    predictor.save()

    # 6. Demonstração de predição
    demonstrar_predicao(predictor, X, meta, feature_cols)

    print("\n[OK] Pipeline de treino concluido com sucesso!")
    print(f"   Modelo salvo em: data/models/predictor.pkl")
    print(f"   Para usar no app, chame: MatchPredictor.load()")


if __name__ == "__main__":
    main()

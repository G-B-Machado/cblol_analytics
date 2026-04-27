import os
import pickle
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline

# XGBoost é opcional — usa Logistic Regression como fallback se não estiver instalado
try:
    from xgboost import XGBClassifier
    XGBOOST_AVAILABLE = True
except ImportError:
    XGBOOST_AVAILABLE = False


class MatchPredictor:
    """
    Encapsula o modelo preditivo de resultado de partidas do CBLOL.

    Suporta dois backends:
    - XGBoost (preferido): captura interações não-lineares entre features
    - Logistic Regression (fallback): mais simples, mais interpretável

    Uso:
        predictor = MatchPredictor()
        predictor.train(X, y)
        proba = predictor.predict_proba_matchup(team_stats_a, team_stats_b)
    """

    MODEL_DIR = "data/models"
    MODEL_PATH = "data/models/predictor.pkl"
    FEATURE_PATH = "data/models/feature_cols.pkl"

    def __init__(self, use_xgboost: bool = True):
        self.use_xgboost = use_xgboost and XGBOOST_AVAILABLE
        self.feature_cols = None
        self.model = None
        self._build_model()

    def _build_model(self):
        if self.use_xgboost:
            print("[MatchPredictor] Backend: XGBoost")
            self.model = XGBClassifier(
                n_estimators=100,
                max_depth=3,
                learning_rate=0.1,
                eval_metric="logloss",
                random_state=42,
            )
        else:
            print("[MatchPredictor] Backend: Logistic Regression (XGBoost não encontrado)")
            self.model = Pipeline([
                ("scaler", StandardScaler()),
                ("clf", LogisticRegression(max_iter=1000, random_state=42)),
            ])

    def train(self, X: pd.DataFrame, y: pd.Series, feature_cols: list):
        """Treina o modelo com o dataset de features gerado pelo FeatureEngineer."""
        self.feature_cols = feature_cols
        self.model.fit(X[feature_cols], y)
        print("[MatchPredictor] Modelo treinado com sucesso.")

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        """Retorna probabilidades para um DataFrame de features já construído."""
        return self.model.predict_proba(X[self.feature_cols])[:, 1]

    def predict_proba_matchup(
        self,
        team_a_stats: dict,
        team_b_stats: dict,
        team_a_side: str = "Blue",
    ) -> dict:
        """
        Prediz a probabilidade de vitória dado dois dicts de stats históricos.

        Args:
            team_a_stats: dict com features históricas do Time A
            team_b_stats: dict com features históricas do Time B
            team_a_side: 'Blue' ou 'Red' para o Time A

        Returns:
            dict com probabilidades: {'team_a': 0.62, 'team_b': 0.38}
        """
        if self.model is None or self.feature_cols is None:
            raise RuntimeError("Modelo não treinado. Chame .train() primeiro ou carregue com .load().")

        side_a = 1 if team_a_side == "Blue" else 0
        side_b = 1 - side_a

        row_a = {**team_a_stats, "side_blue": side_a}
        row_b = {**team_b_stats, "side_blue": side_b}

        df_input = pd.DataFrame([row_a, row_b])

        # Preenche colunas ausentes com 0.5 (neutro)
        for col in self.feature_cols:
            if col not in df_input.columns:
                df_input[col] = 0.5

        probas = self.model.predict_proba(df_input[self.feature_cols])

        # A probabilidade de A ganhar é a média ponderada das predições individuais
        # Usamos a predição do ponto de vista de A e o complemento de B
        p_a_from_a = probas[0][1]
        p_a_from_b = 1 - probas[1][1]
        p_a = (p_a_from_a + p_a_from_b) / 2
        p_b = 1 - p_a

        return {"team_a": round(float(p_a), 4), "team_b": round(float(p_b), 4)}

    def save(self):
        """Salva o modelo treinado e a lista de features em disco."""
        os.makedirs(self.MODEL_DIR, exist_ok=True)
        with open(self.MODEL_PATH, "wb") as f:
            pickle.dump(self.model, f)
        with open(self.FEATURE_PATH, "wb") as f:
            pickle.dump(self.feature_cols, f)
        print(f"[MatchPredictor] Modelo salvo em '{self.MODEL_PATH}'")

    @classmethod
    def load(cls) -> "MatchPredictor":
        """Carrega um modelo previamente treinado do disco."""
        instance = cls.__new__(cls)
        with open(cls.MODEL_PATH, "rb") as f:
            instance.model = pickle.load(f)
        with open(cls.FEATURE_PATH, "rb") as f:
            instance.feature_cols = pickle.load(f)
        print(f"[MatchPredictor] Modelo carregado de '{cls.MODEL_PATH}'")
        return instance

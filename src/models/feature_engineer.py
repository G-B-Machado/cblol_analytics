import pandas as pd
import numpy as np


class FeatureEngineer:
    """
    Transforma o Gold DataFrame em um dataset de treino para o modelo preditivo.
    
    Estratégia:
    - Uma linha por time por partida (2 linhas por jogo: um para cada time)
    - As features são calculadas com base nos N jogos ANTERIORES ao jogo em questão
      (rolling window), evitando data leakage
    - O target é o resultado (1=vitória, 0=derrota)
    """

    def __init__(self, window: int = 5):
        self.window = window

    def _get_team_game_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filtra apenas linhas de resumo de time e ordena cronologicamente."""
        team_df = df[df["position"] == "team"].copy()
        team_df["date"] = pd.to_datetime(team_df["date"])
        team_df = team_df.sort_values(["teamname", "date"]).reset_index(drop=True)
        return team_df

    def _compute_rolling_features(self, team_df: pd.DataFrame) -> pd.DataFrame:
        """Calcula métricas históricas por rolling window para cada time."""

        # Garante que as colunas necessárias existem
        # firstblood e firstinhibitor podem não estar no Gold (vieram do Oracle's Elixir)
        required_cols = ["firstblood", "firstinhibitor", "teamkills", "teamdeaths", "gamelength", "result"]
        for col in required_cols:
            if col not in team_df.columns:
                team_df[col] = np.nan

        team_df["gamelength"] = pd.to_numeric(team_df["gamelength"], errors="coerce")
        team_df["teamkills"] = pd.to_numeric(team_df["teamkills"], errors="coerce")
        team_df["teamdeaths"] = pd.to_numeric(team_df["teamdeaths"], errors="coerce")
        team_df["result"] = pd.to_numeric(team_df["result"], errors="coerce")
        team_df["k_diff"] = team_df["teamkills"] - team_df["teamdeaths"]
        team_df["firstblood"] = pd.to_numeric(team_df["firstblood"], errors="coerce")
        team_df["firstinhibitor"] = pd.to_numeric(team_df["firstinhibitor"], errors="coerce")

        w = self.window

        def rolling_mean(series):
            # Shift 1 para não incluir o jogo atual (evita leakage)
            return series.shift(1).rolling(window=w, min_periods=1).mean()

        grp = team_df.groupby("teamname", group_keys=False)

        team_df[f"win_rate_{w}g"] = grp["result"].transform(rolling_mean)
        team_df[f"fb_rate_{w}g"] = grp["firstblood"].transform(rolling_mean)
        team_df[f"inibi_rate_{w}g"] = grp["firstinhibitor"].transform(rolling_mean)
        team_df[f"avg_k_diff_{w}g"] = grp["k_diff"].transform(rolling_mean)
        team_df[f"avg_duration_{w}g"] = grp["gamelength"].transform(rolling_mean)

        return team_df

    def _compute_h2h(self, team_df: pd.DataFrame) -> pd.DataFrame:
        """Calcula o win rate histórico de cada time contra cada oponente específico."""

        # Primeiro, montamos um mapa gameid -> oponente
        game_opponents = (
            team_df[["gameid", "teamname"]]
            .drop_duplicates()
            .groupby("gameid")["teamname"]
            .apply(list)
            .reset_index()
        )
        # Expandir para ter (gameid, team, opponent)
        h2h_rows = []
        for _, row in game_opponents.iterrows():
            if len(row["teamname"]) == 2:
                h2h_rows.append({"gameid": row["gameid"], "teamname": row["teamname"][0], "opponent": row["teamname"][1]})
                h2h_rows.append({"gameid": row["gameid"], "teamname": row["teamname"][1], "opponent": row["teamname"][0]})

        h2h_map = pd.DataFrame(h2h_rows)
        team_df = team_df.merge(h2h_map, on=["gameid", "teamname"], how="left")

        # Calcular win rate H2H acumulado (shift para não incluir o jogo atual)
        team_df = team_df.sort_values(["teamname", "opponent", "date"])
        team_df["h2h_win_rate"] = (
            team_df.groupby(["teamname", "opponent"])["result"]
            .transform(lambda s: s.shift(1).expanding().mean())
        )

        return team_df

    def build_training_set(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Pipeline completo: Gold DataFrame → Dataset de treino.

        Retorna um DataFrame com uma linha por time por jogo, com:
        - Features históricas de rolling window
        - Feature de lado (blue/red)
        - Feature H2H
        - Target: result
        """
        print("[FeatureEngineer] Iniciando construção do dataset de treino...")

        team_df = self._get_team_game_rows(df)
        team_df = self._compute_rolling_features(team_df)
        team_df = self._compute_h2h(team_df)

        w = self.window
        feature_cols = [
            f"win_rate_{w}g",
            f"fb_rate_{w}g",
            f"inibi_rate_{w}g",
            f"avg_k_diff_{w}g",
            f"avg_duration_{w}g",
            "h2h_win_rate",
        ]

        # Codifica o lado: Blue=1, Red=0
        team_df["side_blue"] = (team_df["side"] == "Blue").astype(int)
        feature_cols.append("side_blue")

        # Remove primeiros jogos onde o rolling ainda não tem dados suficientes
        train_df = team_df.dropna(subset=[f"win_rate_{w}g"]).copy()

        # Preenche H2H missing (primeiro confronto direto) com 0.5 (50/50)
        train_df["h2h_win_rate"] = train_df["h2h_win_rate"].fillna(0.5)

        X = train_df[feature_cols]
        y = train_df["result"].astype(int)
        meta = train_df[["gameid", "teamname", "date", "opponent"]]

        print(f"[FeatureEngineer] Dataset gerado: {len(X)} amostras, {len(feature_cols)} features.")
        return X, y, meta, feature_cols

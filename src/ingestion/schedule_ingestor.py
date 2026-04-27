"""
Módulo de Ingestão de Odds para CBLOL Analytics.

Estratégia adotada:
- APIs de esports (Pandascore) proíbem uso em apostas no tier gratuito.
- The Odds API não cobre CBLOL.
- A solução prática é:
  1. Buscar os PRÓXIMOS JOGOS do CBLOL via Leaguepedia (schedule)
  2. O usuário insere as odds da casa de apostas manualmente no app (Betano, Bet365 etc.)
  3. O sistema calcula automaticamente se é uma Value Bet comparando com o modelo.

Este módulo trata a parte de buscar o schedule de partidas futuras.
"""

import mwclient
import pandas as pd
import time
import os
from datetime import datetime, timezone


class ScheduleIngestor:
    """
    Busca as próximas partidas do CBLOL via Leaguepedia (ScoreboardGames).
    Retorna partidas futuras com data, times e tournament.
    """

    def __init__(self):
        self.user_agent = "CBLOLAnalyticsBot/1.0 (contato@cblol-analytics.com)"
        self.site = mwclient.Site("lol.fandom.com", path="/", clients_useragent=self.user_agent)

    def get_upcoming_matches(self, year: int = 2026, split: str = "Split 1", limit: int = 20) -> pd.DataFrame | None:
        """
        Busca as próximas partidas agendadas do CBLOL.

        Retorna um DataFrame com colunas:
            GameId, Tournament, Team1, Team2, DateTime_UTC, Patch
        """
        print(f"[ScheduleIngestor] Buscando agenda do CBLOL {year} {split}...")

        where_clause = (
            f"SG.Tournament LIKE 'CBLOL {year}%' AND SG.Tournament LIKE '%{split}%'"
        )

        try:
            response = self.site.api(
                "cargoquery",
                tables="ScoreboardGames=SG",
                fields="SG.GameId, SG.Tournament, SG.Team1, SG.Team2, SG.WinTeam, SG.DateTime_UTC, SG.Patch",
                where=where_clause,
                order_by="SG.DateTime_UTC DESC",
                limit=str(limit),
            )

            data = [item["title"] for item in response["cargoquery"]]
            df = pd.DataFrame(data)

            if df.empty:
                print("[ScheduleIngestor] Nenhuma partida encontrada.")
                return None

            df["DateTime_UTC"] = pd.to_datetime(df["DateTime_UTC"], errors="coerce", utc=True)

            # Separar jogos futuros (WinTeam vazio = ainda não jogado)
            upcoming = df[df["WinTeam"].isna() | (df["WinTeam"] == "")]
            played = df[df["WinTeam"].notna() & (df["WinTeam"] != "")]

            print(f"[ScheduleIngestor] {len(upcoming)} partidas futuras | {len(played)} partidas jogadas.")
            return df

        except Exception as e:
            print(f"[ScheduleIngestor] Erro ao consultar Leaguepedia: {e}")
            return None

    def save_schedule(self, df: pd.DataFrame, path: str = "data/raw/schedule.csv"):
        """Salva o schedule em CSV."""
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_csv(path, index=False)
        print(f"[ScheduleIngestor] Schedule salvo em '{path}'")

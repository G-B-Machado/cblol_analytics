"""
Módulo de Identificação de Value Bets.

Uma Value Bet ocorre quando a probabilidade REAL de um evento (estimada pelo modelo)
é MAIOR do que a probabilidade implícita nas odds da casa de apostas.

Fórmulas chave:
  - Probabilidade Implícita da Odd = 1 / odd_decimal
  - Value = (probabilidade_modelo * odd_decimal) - 1
  - Se Value > 0 → Value Bet (aposta com valor positivo esperado)
  - Margem da Casa (Vig) = soma das probabilidades implícitas - 1

Exemplo:
  Modelo diz: LOUD tem 65% de chance de ganhar
  Betano oferece: 1.70 (prob. implícita = 58.8%)
  Value = (0.65 * 1.70) - 1 = 0.105 → +10.5% de valor → APOSTE!
"""

import pandas as pd
from dataclasses import dataclass


@dataclass
class ValueBetResult:
    """Resultado da análise de uma aposta."""
    team: str
    opponent: str
    model_proba: float        # Probabilidade do modelo (0 a 1)
    bookmaker_odd: float      # Odd decimal da casa de apostas
    implied_proba: float      # Probabilidade implícita da odd (1/odd)
    value: float              # Valor esperado: (model_proba * odd) - 1
    is_value_bet: bool        # True se value > threshold
    edge: float               # Vantagem em % sobre a casa
    kelly_fraction: float     # Fração Kelly para gerenciamento de banca
    recommendation: str       # Texto da recomendação

    def to_dict(self) -> dict:
        return {
            "Time": self.team,
            "Oponente": self.opponent,
            "Prob. Modelo": f"{self.model_proba*100:.1f}%",
            "Odd Ofertada": f"{self.bookmaker_odd:.2f}",
            "Prob. Implícita": f"{self.implied_proba*100:.1f}%",
            "Value (EV)": f"{self.value*100:+.1f}%",
            "Edge": f"{self.edge*100:+.1f}%",
            "Kelly %": f"{max(self.kelly_fraction*100, 0):.1f}%",
            "Recomendação": self.recommendation,
        }


class ValueBetFinder:
    """
    Compara as probabilidades do modelo preditivo com as odds das casas de apostas
    para identificar apostas com valor positivo esperado (Value Bets).
    """

    def __init__(self, min_value_threshold: float = 0.03, min_model_proba: float = 0.50):
        """
        Args:
            min_value_threshold: Valor mínimo de EV para considerar Value Bet (padrão: 3%)
            min_model_proba: Probabilidade mínima do modelo para recomendar (padrão: 50%)
        """
        self.min_value_threshold = min_value_threshold
        self.min_model_proba = min_model_proba

    def analyze(
        self,
        team_a: str,
        team_b: str,
        model_proba_a: float,
        odd_a: float,
        odd_b: float,
    ) -> tuple[ValueBetResult, ValueBetResult]:
        """
        Analisa um confronto e retorna os resultados de value bet para ambos os times.

        Args:
            team_a: Nome do Time A
            team_b: Nome do Time B
            model_proba_a: Probabilidade do Time A ganhar (do modelo, 0 a 1)
            odd_a: Odd decimal da casa para o Time A vencer
            odd_b: Odd decimal da casa para o Time B vencer

        Returns:
            Tupla (resultado_time_a, resultado_time_b)
        """
        model_proba_b = 1 - model_proba_a

        result_a = self._calculate_value(team_a, team_b, model_proba_a, odd_a)
        result_b = self._calculate_value(team_b, team_a, model_proba_b, odd_b)

        return result_a, result_b

    def _calculate_value(
        self,
        team: str,
        opponent: str,
        model_proba: float,
        odd: float,
    ) -> ValueBetResult:
        """Calcula o value de uma aposta específica."""

        implied_proba = 1 / odd if odd > 0 else 1.0
        value = (model_proba * odd) - 1
        edge = model_proba - implied_proba

        # Critério de Kelly Fracionado (usa 1/4 do Kelly para maior segurança)
        # Kelly = (p * b - q) / b, onde b = odd - 1
        b = odd - 1
        q = 1 - model_proba
        kelly_full = (model_proba * b - q) / b if b > 0 else 0
        kelly_fraction = kelly_full / 4  # Kelly fracionado (25%)

        # Determina se é value bet
        is_value_bet = (
            value >= self.min_value_threshold
            and model_proba >= self.min_model_proba
            and kelly_fraction > 0
        )

        # Recomendação textual
        if is_value_bet:
            if value >= 0.15:
                recommendation = "FORTE VALUE BET"
            elif value >= 0.08:
                recommendation = "Value Bet moderado"
            else:
                recommendation = "Value Bet fraco"
        else:
            if value > 0:
                recommendation = "Valor positivo, mas abaixo do limiar"
            elif model_proba < self.min_model_proba:
                recommendation = "Probabilidade insuficiente"
            else:
                recommendation = "Sem valor (odds baixas)"

        return ValueBetResult(
            team=team,
            opponent=opponent,
            model_proba=model_proba,
            bookmaker_odd=odd,
            implied_proba=implied_proba,
            value=value,
            is_value_bet=is_value_bet,
            edge=edge,
            kelly_fraction=kelly_fraction,
            recommendation=recommendation,
        )

    def get_vig(self, odd_a: float, odd_b: float) -> float:
        """Calcula a margem da casa (vig/overround)."""
        return (1 / odd_a + 1 / odd_b) - 1

    def summarize(self, result_a: ValueBetResult, result_b: ValueBetResult) -> pd.DataFrame:
        """Retorna um DataFrame formatado com os resultados de ambos os times."""
        rows = [result_a.to_dict(), result_b.to_dict()]
        return pd.DataFrame(rows).set_index("Time")

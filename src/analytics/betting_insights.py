import pandas as pd
import numpy as np


class BettingAnalyzer:
    def __init__(self):
        pass

    def get_team_stats(self, df):
        """Calcula métricas fundamentais por time. Defensivo a colunas ausentes."""
        team_df = df[df['position'] == 'team'].copy()

        # Garante que as colunas existem antes de usar (nem sempre vêm do Gold)
        optional_numeric = ['gamelength', 'firstblood', 'firstinhibitor', 'teamkills', 'teamdeaths']
        for col in optional_numeric:
            if col not in team_df.columns:
                team_df[col] = np.nan
            else:
                team_df[col] = pd.to_numeric(team_df[col], errors='coerce')

        # Monta o dicionário de agregações dinamicamente
        agg_dict = {
            'jogos': ('gameid', 'count'),
            'vitorias': ('result', 'sum'),
        }

        agg_dict['fb_total'] = ('firstblood', 'sum')
        agg_dict['inibi_total'] = ('firstinhibitor', 'sum')
        agg_dict['avg_duration'] = ('gamelength', 'mean')
        agg_dict['kills_total'] = ('teamkills', 'sum')
        agg_dict['deaths_total'] = ('teamdeaths', 'sum')

        stats = team_df.groupby('teamname').agg(**agg_dict)

        # Métricas calculadas
        stats['win_rate'] = (stats['vitorias'] / stats['jogos'] * 100).round(1)
        stats['fb_rate'] = (stats['fb_total'] / stats['jogos'] * 100).round(1)
        stats['inibi_rate'] = (stats['inibi_total'] / stats['jogos'] * 100).round(1)
        stats['avg_k_diff'] = ((stats['kills_total'] - stats['deaths_total']) / stats['jogos']).round(1)

        return stats
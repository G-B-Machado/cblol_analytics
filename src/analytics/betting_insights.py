import pandas as pd

class BettingAnalyzer:
    def __init__(self):
        pass

    def get_team_stats(self, df):
        """Calcula métricas fundamentais por time"""
        # Filtra apenas as linhas de resumo de time
        team_df = df[df['position'] == 'team'].copy()
        print(list(team_df.columns))
        # Converte gamelength para minutos (numérico) se necessário
        if team_df['gamelength'].dtype == object:
            team_df['gamelength'] = team_df['gamelength'].astype(float)

        stats = team_df.groupby('teamname').agg(
            jogos=('gameid', 'count'),
            vitorias=('result', 'sum'),
            fb_total=('firstblood', 'sum'),
            inibi_total=('firstinhibitor', 'sum'),
            avg_duration=('gamelength', 'mean'),
            kills_total=('teamkills', 'sum'),
            deaths_total=('teamdeaths', 'sum')
        )
        
        # Métricas calculadas
        stats['win_rate'] = (stats['vitorias'] / stats['jogos'] * 100).round(1)
        stats['fb_rate'] = (stats['fb_total'] / stats['jogos'] * 100).round(1)
        stats['inibi_rate'] = (stats['inibi_total'] / stats['jogos'] * 100).round(1)
        stats['avg_k_diff'] = ((stats['kills_total'] - stats['deaths_total']) / stats['jogos']).round(1)
        
        return stats
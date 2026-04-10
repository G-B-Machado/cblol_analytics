import pandas as pd

class DraftAnalyzer:
    def __init__(self, gold_path="data/gold"):
        self.gold_path = gold_path

    def get_winrate_by_side(self, df):
        """Calcula a vantagem de Side (Blue vs Red)"""
        # Pegamos apenas uma linha por jogo para não duplicar a contagem
        match_results = df.drop_duplicates(subset=['gameid', 'side'])
        side_stats = match_results.groupby('side')['result'].mean() * 100
        return side_stats

    def get_champion_presence(self, df):
        """Calcula Pick/Ban Rate e Win Rate por Campeão"""
        # 1. Win Rate de Picks
        player_data = df[df['position'] != 'team']
        picks = player_data.groupby('champion').agg(
            games_played=('result', 'count'),
            wins=('result', 'sum')
        )
        picks['win_rate'] = (picks['wins'] / picks['games_played'] * 100).round(2)

        # 2. Análise de Bans (Dados da Leaguepedia que trouxemos no Join)
        # Os bans vêm em strings separadas por vírgula no nosso Gold
        all_bans = []
        ban_cols = ['Team1Bans', 'Team2Bans']
        
        # Pegamos uma linha por gameid para processar os bans
        unique_games = df.drop_duplicates('gameid')
        
        for col in ban_cols:
            bans_series = unique_games[col].dropna().str.split(',')
            for ban_list in bans_series:
                all_bans.extend([b.strip() for b in ban_list if b.strip()])
        
        ban_counts = pd.Series(all_bans).value_counts().to_frame('times_banned')
        
        # 3. Join de Picks e Bans
        report = picks.join(ban_counts, how='outer').fillna(0)
        report['presence'] = ((report['games_played'] + report['times_banned']) / unique_games['gameid'].nunique() * 100).round(2)
        
        return report.sort_values(by='presence', ascending=False)
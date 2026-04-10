import pandas as pd
import numpy as np
import os

class DataJoiner:
    def __init__(self, gold_path="data/gold"):
        self.gold_path = gold_path

    def _generate_match_key(self, df, t1_col, t2_col, date_col):
        temp_df = df.copy()
        temp_df['date_str'] = pd.to_datetime(temp_df[date_col]).dt.strftime('%Y-%m-%d')
        teams = np.sort(temp_df[[t1_col, t2_col]].values, axis=1)
        return temp_df['date_str'] + "_" + teams[:, 0] + "_vs_" + teams[:, 1]

    def join_sources(self, silver_df, lp_df):
        lp_processed = lp_df.copy()
        # Identificar a coluna de data correta (ajuste para o KeyError)
        date_col = 'DateTime_UTC' if 'DateTime_UTC' in lp_processed.columns else 'DateTime UTC'
        
        if date_col not in lp_processed.columns:
            # Fallback caso a API mude o nome drasticamente
            print(f"[!] Colunas disponíveis na Leaguepedia: {lp_processed.columns}")
            raise KeyError("Coluna de data não encontrada no retorno da Leaguepedia")

        lp_processed['match_key'] = self._generate_match_key(lp_processed, 'Team1', 'Team2', date_col)
        
        mapping = silver_df[silver_df['position'] == 'team'][['gameid', 'teamname']].drop_duplicates()
        match_info = []
        for gid, group in mapping.groupby('gameid'):
            if len(group) == 2:
                teams = sorted(group['teamname'].tolist())
                match_info.append({'gameid': gid, 'opponent_key': f"{teams[0]}_vs_{teams[1]}"})
        
        silver_with_key = silver_df.merge(pd.DataFrame(match_info), on='gameid', how='left')
        silver_with_key['match_key'] = pd.to_datetime(silver_with_key['date']).dt.strftime('%Y-%m-%d') + "_" + silver_with_key['opponent_key']

        lp_cols = ['match_key', 'Patch', 'Team1Bans', 'Team2Bans']
        return silver_with_key.merge(lp_processed[lp_cols], on='match_key', how='left')

    def save_gold_data(self, df, filename):
        if not os.path.exists(self.gold_path): os.makedirs(self.gold_path)
        df.to_csv(os.path.join(self.gold_path, filename), index=False)
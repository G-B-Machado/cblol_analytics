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
        print("[*] Iniciando Joiner: Cruzando Performance com Metadados...")
        
        # 1. PREPARAR LEAGUEPEDIA (LADO '1' DO JOIN)
        lp_processed = lp_df.copy()
        
        # Identifica a coluna de data (ajuste para o erro que tivemos antes)
        date_col = 'DateTime_UTC' if 'DateTime_UTC' in lp_processed.columns else 'DateTime UTC'
        
        # Gera a chave única na Leaguepedia
        lp_processed['match_key'] = self._generate_match_key(lp_processed, 'Team1', 'Team2', date_col)
        
        # --- AQUI ESTÁ A CORREÇÃO PARA AS 200 LINHAS EXTRA ---
        # Garantimos que só existe UMA linha por chave na Leaguepedia antes do merge
        lp_unique = lp_processed.drop_duplicates(subset=['match_key'], keep='first')
        
        if len(lp_unique) < len(lp_processed):
            diff = len(lp_processed) - len(lp_unique)
            print(f"[!] Aviso: Removidas {diff} linhas duplicadas na Leaguepedia para evitar explosão no Join.")

        # 2. PREPARAR ORACLE (LADO 'N' DO JOIN)
        # Identificar oponentes para criar a match_key no Silver
        mapping = silver_df[silver_df['position'] == 'team'][['gameid', 'teamname']].drop_duplicates()
        match_info = []
        for gid, group in mapping.groupby('gameid'):
            if len(group) == 2:
                teams = sorted(group['teamname'].tolist())
                match_info.append({'gameid': gid, 'opponent_key': f"{teams[0]}_vs_{teams[1]}"})
        
        silver_with_key = silver_df.merge(pd.DataFrame(match_info), on='gameid', how='left')
        silver_with_key['match_key'] = pd.to_datetime(silver_with_key['date']).dt.strftime('%Y-%m-%d') + "_" + silver_with_key['opponent_key']

        # 3. MERGE FINAL (M:1)
        # Usamos validate='m:1' para que o pandas nos avise se a lógica falhar novamente
        lp_cols = ['match_key', 'Patch', 'Team1Bans', 'Team2Bans']
        
        gold_df = silver_with_key.merge(
            lp_unique[lp_cols], 
            on='match_key', 
            how='left',
            validate='m:1' 
        )

        print(f"[+] Camada Gold gerada com {len(gold_df)} linhas.")
        return gold_df

    def save_gold_data(self, df, filename):
        if not os.path.exists(self.gold_path): os.makedirs(self.gold_path)
        df.to_csv(os.path.join(self.gold_path, filename), index=False)
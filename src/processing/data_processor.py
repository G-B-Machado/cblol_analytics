import pandas as pd
import os

class DataProcessor:
    def __init__(self, processed_path="data/processed"):
        self.processed_path = processed_path
        self.team_mapping = {
            "VKS": "Vivo Keyd Stars", "PNG": "paiN Gaming", "LLL": "LOUD",
            "RED": "RED Canids Kalunga", "FX": "Fluxo", "FUR": "FURIA",
            "ITZ": "INTZ", "KBM": "KaBuM! Esports", "LOS": "LOS", "Liberty": "Liberty"
        }

    def clean_oracles_data(self, df):
        if df is None: return None
        
        essential_cols = [
            'gameid', 'league', 'year', 'split', 'date', 'patch', 'side', 
            'position', 'playername', 'teamname', 'champion', 'result','gamelength', 
            'firstblood', 'firstinhibitor', 'teamkills', 'teamdeaths'

        ]
        
        cols_to_keep = [c for c in essential_cols if c in df.columns]
        df_clean = df[cols_to_keep].copy()
        df_clean['teamname'] = df_clean['teamname'].replace(self.team_mapping)
        df_clean['date'] = pd.to_datetime(df_clean['date'])
        
        return df_clean

    def save_processed_data(self, df, year, split):
        if not os.path.exists(self.processed_path):
            os.makedirs(self.processed_path)
        filename = f"cblol_{year}_{split}_silver.csv"
        path = os.path.join(self.processed_path, filename)
        df.to_csv(path, index=False)
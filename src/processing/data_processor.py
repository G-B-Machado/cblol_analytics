import pandas as pd
import json
import os

class DataProcessor:
    def __init__(self, processed_path="data/processed"):
        self.processed_path = processed_path
        # Dicionário de normalização (pode ser movido para um ficheiro config/ later)
        self.team_mapping = {
            "VKS": "Vivo Keyd Stars",
            "PNG": "paiN Gaming",
            "LLL": "LOUD",
            "RED": "RED Canids Kalunga",
            "FX": "Fluxo",
            "FUR": "FURIA",
            "ITZ": "INTZ",
            "KBM": "KaBuM! Esports",
            "LOS": "LOS",
            "Liberty": "Liberty"
        }

    def clean_oracles_data(self, df):
        """Limpa e normaliza os dados brutos do Oracle's Elixir"""
        if df is None:
            return None
        
        print("[*] A iniciar limpeza dos dados...")
        
        # 1. Selecionar apenas colunas essenciais para análise global
        # Adicione ou remova colunas conforme a necessidade dos seus eixos de análise
        essential_cols = [
            'gameid', 'league', 'year', 'split', 'date', 'patch', 'side', 
            'position', 'playername', 'teamname', 'champion', 'result', 
            'gamelength', 'kills', 'deaths', 'assists', 'goldat15', 'xpat15', 
            'csat15', 'golddiffat15', 'xpdiffat15', 'csdiffat15', 'killsat15', 
            'deathsat15', 'assistsat15'
        ]
        
        # Filtrar colunas existentes no DF
        cols_to_keep = [c for c in essential_cols if c in df.columns]
        df_clean = df[cols_to_keep].copy()

        # 2. Normalizar nomes de equipas
        df_clean['teamname'] = df_clean['teamname'].replace(self.team_mapping)

        # 3. Converter tipos de dados
        df_clean['date'] = pd.to_datetime(df_clean['date'])
        
        print(f"[+] Limpeza concluída. {len(df_clean)} registos processados.")
        return df_clean

    def save_processed_data(self, df, filename="cblol_silver.csv"):
        if not os.path.exists(self.processed_path):
            os.makedirs(self.processed_path)
            
        save_path = os.path.join(self.processed_path, filename)
        df.to_csv(save_path, index=False)
        print(f"[+] Dados normalizados salvos em: {save_path}")
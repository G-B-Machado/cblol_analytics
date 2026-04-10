import pandas as pd
import os
import gdown

class OraclesElixirIngestor:
    def __init__(self, raw_data_path="data/raw"):
        self.raw_data_path = raw_data_path
        self.file_id_2026 = "1hnpbrUpBMS1TZI7IovfpKeZfWJH1Aptm" 
        if not os.path.exists(self.raw_data_path):
            os.makedirs(self.raw_data_path)

    def download_from_drive(self, year=2026, split="Split 1"):
        url = f'https://drive.google.com/uc?id={self.file_id_2026}'
        temp_path = os.path.join(self.raw_data_path, "temp_download.csv")
        
        try:
            gdown.download(url, temp_path, quiet=False)
            df = pd.read_csv(temp_path)
            cblol_df = df[df['league'] == 'CBLOL'].copy()
            
            filename = f"oe_{year}_{split}_raw.csv"
            save_path = os.path.join(self.raw_data_path, filename)
            cblol_df.to_csv(save_path, index=False)
            
            if os.path.exists(temp_path):
                os.remove(temp_path)
            return cblol_df
        except Exception as e:
            print(f"[!] Erro no download: {e}")
            return None

    def load_local_raw(self, year=2026, split="Split 1"):
        filename = f"oe_{year}_{split}_raw.csv"
        path = os.path.join(self.raw_data_path, filename)
        if os.path.exists(path):
            return pd.read_csv(path)
        return None
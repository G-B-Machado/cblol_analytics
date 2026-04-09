import pandas as pd
import os
import gdown
from datetime import datetime

class OraclesElixirIngestor:
    def __init__(self, raw_data_path="data/raw"):
        self.raw_data_path = raw_data_path
        # Para o ano de 2026, você deve identificar o ID do arquivo .csv dentro da pasta do Drive
        # O ID é a parte final da URL de compartilhamento do arquivo específico
        self.file_id_2026 = "1hnpbrUpBMS1TZI7IovfpKeZfWJH1Aptm" 

    def download_from_drive(self, file_id=None):
        """Faz o download do CSV diretamente do Google Drive"""
        fid = file_id or self.file_id_2026
        url = f'https://drive.google.com/uc?id={fid}'
        
        # Define o caminho temporário e final
        temp_path = os.path.join(self.raw_data_path, "temp_download.csv")
        
        print(f"[*] Baixando dados do Google Drive (ID: {fid})...")
        
        try:
            # gdown lida bem com arquivos grandes do Drive
            gdown.download(url, temp_path, quiet=False)
            
            # Carrega e filtra apenas CBLOL
            df = pd.read_csv(temp_path)
            cblol_df = df[df['league'] == 'CBLOL'].copy()
            
            # Salva com timestamp para histórico
            filename = f"cblol_raw_{datetime.now().strftime('%Y%m%d')}.csv"
            save_path = os.path.join(self.raw_data_path, filename)
            cblol_df.to_csv(save_path, index=False)
            
            # Remove arquivo temporário
            if os.path.exists(temp_path):
                os.remove(temp_path)
                
            print(f"[+] Sucesso! Dados salvos em: {save_path}")
            return cblol_df
            
        except Exception as e:
            print(f"[!] Erro no processamento do Drive: {e}")
            return None

    def load_local_raw(self):
        """Busca o arquivo filtrado mais recente na pasta raw"""
        if not os.path.exists(self.raw_data_path):
            os.makedirs(self.raw_data_path)
            
        files = [f for f in os.listdir(self.raw_data_path) if f.startswith('cblol_raw')]
        if not files:
            return None
        
        latest_file = sorted(files)[-1]
        print(f"[*] Carregando cache local: {latest_file}")
        return pd.read_csv(os.path.join(self.raw_data_path, latest_file))
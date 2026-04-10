import os
import pandas as pd
from src.ingestion.oracles_elixir import OraclesElixirIngestor
from src.ingestion.leaguepedia import LeaguepediaIngestor
from src.processing.data_processor import DataProcessor

# Configuração Centralizada
CURRENT_YEAR = 2026
CURRENT_SPLIT = "Split 1"

def main():
    processor = DataProcessor()
    processed_file = os.path.join(processor.processed_path, "cblol_silver.csv")
    
    # --- 1. TENTA CARREGAR DADOS JÁ PROCESSADOS (SILVER) ---
    if os.path.exists(processed_file):
        print(f"[*] Carregando dados já processados de {CURRENT_YEAR}...")
        clean_data = pd.read_csv(processed_file)
    else:
        # --- 2. SE NÃO EXISTIR, BUSCA/BAIXA RAW E PROCESSA ---
        oe_ingestor = OraclesElixirIngestor()
        raw_data = oe_ingestor.load_local_raw()
        
        if raw_data is None:
            raw_data = oe_ingestor.download_from_drive() # Usa o file_id_2026 interno
            
        if raw_data is not None:
            clean_data = processor.clean_oracles_data(raw_data)
            processor.save_processed_data(clean_data)
        else:
            print("[!] Falha crítica: Não foi possível obter dados do Oracle's Elixir.")
            return

    # --- 3. ENRIQUECIMENTO (LEAGUEPEDIA) ---
    # Aqui garantimos que o ano seja o mesmo
    lp_ingestor = LeaguepediaIngestor()
    lp_path = "data/raw/leaguepedia_2026_raw.csv"
    
    if os.path.exists(lp_path):
        print("[*] Carregando metadados locais da Leaguepedia...")
        lp_df = pd.read_csv(lp_path)
    else:
        lp_df = lp_ingestor.get_cblol_matches(year=CURRENT_YEAR, split=CURRENT_SPLIT)
        if lp_df is not None:
            lp_df.to_csv(lp_path, index=False)

    print(f"\n[Status] Dataset pronto para análise. Ano: {CURRENT_YEAR}")
    print(f"Linhas em Silver: {len(clean_data)}")

if __name__ == "__main__":
    main()
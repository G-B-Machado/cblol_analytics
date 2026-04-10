import os
import pandas as pd
from src.ingestion.oracles_elixir import OraclesElixirIngestor
from src.ingestion.leaguepedia import LeaguepediaIngestor
from src.processing.data_processor import DataProcessor
from src.processing.joiner import DataJoiner
from src.analytics.draft_engine import DraftAnalyzer

YEAR = 2026
SPLIT = "Split 1"

def main():
    oe_ingestor = OraclesElixirIngestor()
    lp_ingestor = LeaguepediaIngestor()
    processor = DataProcessor()
    joiner = DataJoiner()

    # 1. Oracle's (Silver)
    silver_path = f"data/processed/cblol_{YEAR}_{SPLIT}_silver.csv"
    if os.path.exists(silver_path):
        silver_df = pd.read_csv(silver_path)
    else:
        raw_oe = oe_ingestor.load_local_raw(YEAR, SPLIT) or oe_ingestor.download_from_drive(YEAR, SPLIT)
        silver_df = processor.clean_oracles_data(raw_oe)
        if silver_df is not None:
            processor.save_processed_data(silver_df, YEAR, SPLIT)

    # 2. Leaguepedia (Raw)
    lp_path = f"data/raw/lp_{YEAR}_{SPLIT}_raw.csv"
    if os.path.exists(lp_path):
        lp_df = pd.read_csv(lp_path)
    else:
        lp_df = lp_ingestor.get_cblol_matches(year=YEAR, split=SPLIT)
        if lp_df is not None: lp_df.to_csv(lp_path, index=False)

    # 3. Gold
    if silver_df is not None and lp_df is not None:
        gold_df = joiner.join_sources(silver_df, lp_df)
        joiner.save_gold_data(gold_df, f"cblol_{YEAR}_{SPLIT}_gold.csv")
        print("[!] Sucesso: Camada Gold gerada.")

    # 4. ANALYTICS
    analyzer = DraftAnalyzer()
    print("\n" + "="*30)
    print("📊 RELATÓRIO DE ANALYTICS - CBLOL 2026")
    print("="*30)

    # Estatística de Side
    side_winrate = analyzer.get_winrate_by_side(gold_df)
    print(f"\n[Side Winrate]\n{side_winrate}")

    # Top Presence (Picks + Bans)
    presence = analyzer.get_champion_presence(gold_df)
    print("\n[Top 5 Prioridades do Meta (P/B)]")
    print(presence[['presence', 'win_rate', 'games_played', 'times_banned']].head(5))

if __name__ == "__main__":
    main()
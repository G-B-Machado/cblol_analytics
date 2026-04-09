from src.ingestion.oracles_elixir import OraclesElixirIngestor
from src.processing.data_processor import DataProcessor

def main():
    ingestor = OraclesElixirIngestor()
    
    # Tenta carregar local primeiro, se não tiver, baixa
    data = ingestor.load_local_raw()
    
    if data is None:
        data = ingestor.download_from_drive()
    
    if data is not None:
        print(f"\n[Resumo dos Dados]")
        print(f"Total de linhas capturadas: {len(data)}")
        print(f"Colunas disponíveis: {list(data.columns)[:10]}...") # Primeiras 10 colunas
        print(f"Times encontrados: {data['teamname'].unique()}")

    processor = DataProcessor()
    clean_data = processor.clean_oracles_data(data)
    
    if clean_data is not None:
        processor.save_processed_data(clean_data)
        
        # Exemplo de verificação rápida
        print("\n[Amostra de Dados Normalizados]")
        print(clean_data[['date', 'teamname', 'playername', 'result']].head())

if __name__ == "__main__":
    main()
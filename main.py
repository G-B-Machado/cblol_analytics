from src.ingestion.oracles_elixir import OraclesElixirIngestor

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

if __name__ == "__main__":
    main()
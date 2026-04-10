import mwclient
import pandas as pd
import time
import os

class LeaguepediaIngestor:
    def __init__(self):
        # DICA DE OURO: Identificação amigável para evitar blocks automáticos
        self.user_agent = 'CBLOLAnalyticsBot/1.0 (seu-email@exemplo.com)'
        self.site = mwclient.Site('lol.fandom.com', path='/', clients_useragent=self.user_agent)
        
    def get_cblol_matches(self, year=2026, split="Split 1", max_retries=5):
        """Busca dados com estratégia de Retry e Backoff Exponencial"""
        print(f"[*] Consultando Leaguepedia para {year} {split}...")
        
        # Filtro de torneio
        where_clause = f"SG.Tournament LIKE 'CBLOL {year}%' AND SG.Tournament LIKE '%{split}%'"
        
        attempt = 0
        while attempt < max_retries:
            try:
                response = self.site.api('cargoquery',
                    tables="ScoreboardGames=SG",
                    fields="SG.GameId, SG.Tournament, SG.Team1, SG.Team2, SG.WinTeam, SG.Patch, SG.Team1Bans, SG.Team2Bans, SG.DateTime_UTC",
                    where=where_clause,
                    order_by="SG.DateTime_UTC DESC"
                )
                
                data = [item['title'] for item in response['cargoquery']]
                df = pd.DataFrame(data)
                
                if not df.empty:
                    print(f"[+] Sucesso: {len(df)} partidas capturadas.")
                    return df
                return None

            except Exception as e:
                attempt += 1
                error_msg = str(e).lower()
                
                if 'ratelimited' in error_msg or '503' in error_msg:
                    # BACKOFF EXPONENCIAL: 30s, 60s, 120s, 240s...
                    wait_time = (2 ** (attempt - 1)) * 30 
                    print(f"[!] Rate Limit atingido. Tentativa {attempt}/{max_retries}.")
                    print(f"[!] Aguardando {wait_time}s para tentar novamente...")
                    time.sleep(wait_time)
                else:
                    print(f"[!] Erro inesperado na Leaguepedia: {e}")
                    break
        
        print("[!] Limite de tentativas excedido para a Leaguepedia.")
        return None
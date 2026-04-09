import mwclient
import pandas as pd

class LeaguepediaIngestor:
    def __init__(self):
        self.site = mwclient.Site('lol.fandom.com', path='/')
        
    def get_cblol_matches(self, year=2026, split="Split 1"):
        """Busca dados de partidas e banimentos do CBLOL via Cargo Query"""
        print(f"[*] Consultando Leaguepedia para {year} {split}...")
        
        # Tabelas: ScoreboardGames (Geral) e ScoreboardPlayers (Individual)
        # Vamos focar na ScoreboardGames para pegar os Bans e o Patch
        where_clause = f"SG.Tournament LIKE 'CBLOL {year}%' AND SG.Tournament LIKE '%{split}%'"
        
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
                print(f"[+] {len(df)} partidas encontradas na Leaguepedia.")
                return df
            else:
                print("[!] Nenhuma partida encontrada para os critérios informados.")
                return None
                
        except Exception as e:
            print(f"[!] Erro ao consultar Leaguepedia: {e}")
            return None
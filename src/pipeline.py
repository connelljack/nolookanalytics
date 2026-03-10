import sqlite3
import pandas as pd
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.endpoints import playergamelogs
from nba_api.stats.endpoints import commonplayerinfo
import time
from nba_api.stats.static import teams
from database import get_connection


def insert_teams():
    all_teams = teams.get_teams()
    conn = get_connection()
    cursor = conn.cursor()

    for team in all_teams:
        cursor.execute("""
                   INSERT OR IGNORE INTO teams (team_id, team_name, location, abbreviation)
                   VALUES (?,?,?,?)
                   """, (team['id'], team['nickname'], team['city'], team['abbreviation']))
        
    
    conn.commit()
    conn.close()

"""def explore_games():
    gamefinder = leaguegamefinder.LeagueGameFinder(season_nullable='2025-26', timeout=60)
    df = gamefinder.get_data_frames()[0]
    home_df = df[df['MATCHUP'].str.contains(' vs. ')]
    away_df = df[df['MATCHUP'].str.contains(' @ ')]
    games_df = home_df.merge(away_df, on='GAME_ID', suffixes=('_home', '_away'))
    print(games_df.columns.tolist())"""

def insert_games(df):
    home_df = df[df['MATCHUP'].str.contains(' vs. ')]
    away_df = df[df['MATCHUP'].str.contains(' @ ')]
    games_df = home_df.merge(away_df, on='GAME_ID', suffixes=('_home', '_away'))
    conn = get_connection()
    cursor = conn.cursor()

    for _, game in games_df.iterrows():
        cursor.execute("""
                   INSERT OR IGNORE INTO games (game_id, home_team_id, away_team_id, game_date, home_score, away_score)
                   VALUES (?,?,?,?,?,?)
                   """, (game['GAME_ID'], game['TEAM_ID_home'], game['TEAM_ID_away'], game['GAME_DATE_home'], game['PTS_home'], game['PTS_away']))
        
    
    conn.commit()
    conn.close()

def insert_team_stats(dfts):
    conn = get_connection()
    cursor = conn.cursor()

    for _, game in dfts.iterrows():
        cursor.execute("""
                   INSERT OR IGNORE INTO team_stats (game_id, team_id, season_id, wl, min, pts, fgm, fga, fg3m, fg3a, ftm, fta, drb, orb, asts, stl, blk, tov, pf, plus_minus)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   """, (game['GAME_ID'], game['TEAM_ID'], game['SEASON_ID'], game['WL'], game['MIN'], game['PTS'], game['FGM'], game['FGA'], game['FG3M'], game['FG3A'], game['FTM'], game['FTA'], game['DREB'], game['OREB'], game['AST'], game['STL'], game['BLK'], game['TOV'], game['PF'], game['PLUS_MINUS']))
        
    
    conn.commit()
    conn.close()

def insert_player_stats(df):
    conn = get_connection()
    cursor = conn.cursor()

    for _, row in df.iterrows():
        cursor.execute("""
                   INSERT OR IGNORE INTO player_stats (player_id, game_id, team_id, pts, fgm, fga, fg3m, fg3a, ftm, fta, drb, orb, ast, tov, stl, blk, pf, pfd, min, plus_minus, dd2, td3)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   """, (row['PLAYER_ID'], row['GAME_ID'], row['TEAM_ID'], row['PTS'], row['FGM'], row['FGA'], row['FG3M'], row['FG3A'], row['FTM'], row['FTA'], row['DREB'], row['OREB'], row['AST'], row['TOV'], row['STL'], row['BLK'], row['PF'], row.get('PFD', None), row['MIN'], row.get('PLUS_MINUS', None), row.get('DD2', None), row.get('TD3', None)))
        
    
    conn.commit()
    conn.close()

def insert_players():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT ps.player_id 
        FROM player_stats ps 
        LEFT JOIN players p ON ps.player_id = p.player_id 
        WHERE p.player_id IS NULL
    """)
    player_ids = [row['player_id'] for row in cursor.fetchall()]
    total = len(player_ids)

    for idx, player_id in enumerate(player_ids, 1):
        retries = 3
        for attempt in range(retries):
            try:
                info = commonplayerinfo.CommonPlayerInfo(player_id=player_id, timeout=60)
                df = info.get_data_frames()[0]
                if not df.empty:
                    player_info = df.iloc[0]
                    values = (
                        int(player_info['PERSON_ID']),
                        player_info['DISPLAY_FIRST_LAST'],
                        int(player_info['TEAM_ID']),
                        player_info['POSITION'],
                        player_info['HEIGHT'],
                        player_info['WEIGHT'],
                        player_info['BIRTHDATE'],
                        player_info['JERSEY'],
                        player_info['COUNTRY']
                    )
                    cursor.execute("""
                        INSERT OR IGNORE INTO players 
                            (player_id, player_name, team_id, position, height, weight, birthdate, jersey_number, country)
                        VALUES (?,?,?,?,?,?,?,?,?)
                    """, values)

                    # Commit after every successful insert so if we crash,
                    # progress is saved and the resume query skips this player next run
                    conn.commit()
                    print(f"[{idx}/{total}] Inserted player {player_id}")
                    time.sleep(1)
                    break

            except Exception as e:
                # Exponential backoff — wait longer on each retry
                # attempt 0 -> 5s, attempt 1 -> 10s, attempt 2 -> 15s
                wait = 5 * (attempt + 1)
                print(f"[{idx}/{total}] Player {player_id} attempt {attempt + 1} failed: {e}. Retrying in {wait}s...")
                time.sleep(wait)
        else:
            print(f"[{idx}/{total}] Player {player_id} SKIPPED after {retries} failures")

    conn.close()

"""def explore_players():
    logs = playergamelogs.PlayerGameLogs(season_nullable='2025-26', timeout=60)
    df = logs.get_data_frames()[0]
    print(df.columns.tolist())
    print(df.head(2).to_string())

explore_players()

def explore_player_info():
    # Jayson Tatum's player ID
    info = commonplayerinfo.CommonPlayerInfo(player_id=1628369, timeout=60)
    df = info.get_data_frames()[0]
    print(df.columns.tolist())
    print(df.iloc[0].to_string())

explore_player_info()"""



def run_pipeline():
    print("Fetching data from NBA API...")
    gamefinder = leaguegamefinder.LeagueGameFinder(season_nullable='2025-26', timeout=60)
    df = gamefinder.get_data_frames()[0]
    
    print("Inserting teams...")
    insert_teams()
    
    print("Inserting games...")
    insert_games(df)
    
    print("Inserting team stats...")
    insert_team_stats(df)

    print("Fetching player data...")
    player_logs = playergamelogs.PlayerGameLogs(season_nullable='2025-26', timeout=60)
    player_df = player_logs.get_data_frames()[0]
    
    print("Inserting player stats...")
    insert_player_stats(player_df)

    insert_players()
    
    print("Pipeline complete!")

if __name__ == "__main__":
    run_pipeline()
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), '..','db', 'nba.db')

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def create_tables():
    with get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS teams (
                       team_id INTEGER PRIMARY KEY,
                       team_name TEXT NOT NULL,
                       location TEXT NOT NULL,
                       abbreviation TEXT NOT NULL,
                       conference,
                       division
                       )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS games (
                       game_id TEXT PRIMARY KEY,
                       game_date TEXT NOT NULL,
                       home_team_id INTEGER NOT NULL,
                       away_team_id INTEGER NOT NULL,
                       home_score INTEGER NOT NULL,
                       away_score INTEGER NOT NULL
                       )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS players (
                       player_id INTEGER PRIMARY KEY,
                       player_name TEXT NOT NULL,
                       team_id INTEGER,
                       position TEXT NOT NULL,
                        height TEXT NOT NULL,
                        weight TEXT,
                       birthdate TEXT,
                       jersey_number TEXT,
                       country TEXT
                       )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS player_stats (
                       player_id INTEGER NOT NULL,
                       game_id TEXT NOT NULL,
                       team_id INTEGER NOT NULL,
                       pts INTEGER NOT NULL,
                       fga INTEGER NOT NULL,
                       fgm INTEGER NOT NULL,
                       fg3a INTEGER NOT NULL,
                       fg3m INTEGER NOT NULL,
                       fta INTEGER NOT NULL,
                       ftm INTEGER NOT NULL,
                       drb INTEGER NOT NULL,
                       orb INTEGER NOT NULL,
                       ast INTEGER NOT NULL,
                       tov INTEGER NOT NULL,
                       stl INTEGER NOT NULL,
                       blk INTEGER NOT NULL,
                       pf INTEGER NOT NULL,
                       pfd INTEGER,
                       min TEXT NOT NULL,
                       plus_minus INTEGER,
                       dd2 INTEGER,
                       td3 INTEGER,
                       PRIMARY KEY (player_id, game_id)
                       )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS play_by_play (
                          play_id INTEGER PRIMARY KEY AUTOINCREMENT,
                          game_id TEXT NOT NULL,
                          team_id INTEGER NOT NULL,
                          player_id INTEGER,
                          event_type TEXT NOT NULL,
                          description TEXT NOT NULL,
                          quarter INTEGER NOT NULL,
                          score_home INTEGER NOT NULL,
                          score_away INTEGER NOT NULL,
                          time_remaining TEXT NOT NULL  
                          )
        ''')  

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS team_stats (
                            game_id TEXT NOT NULL,
                            team_id INTEGER NOT NULL,
                            season_id INTEGER NOT NULL,
                            min INTEGER NOT NULL,
                            pts INTEGER NOT NULL,
                            fgm INTEGER NOT NULL,
                            fga INTEGER NOT NULL,
                            fg3m INTEGER NOT NULL,
                            fg3a INTEGER NOT NULL,
                            ftm INTEGER NOT NULL,
                            fta INTEGER NOT NULL,
                            drb INTEGER NOT NULL,
                            orb INTEGER NOT NULL,
                            asts INTEGER NOT NULL,
                            tov INTEGER NOT NULL,
                            stl INTEGER NOT NULL,
                            blk INTEGER NOT NULL,
                            pf INTEGER NOT NULL,
                            plus_minus INTEGER, 
                            wl TEXT NOT NULL,
                            PRIMARY KEY (game_id, team_id)
                          )
        ''')  
# --- NEW TABLE ---
        # One row per field goal attempt. This is the foundation of the xPPS model.
        #
        # Why a separate table instead of adding columns to player_stats?
        # player_stats is one row per player per game — it's aggregated.
        # For xPPS we need one row per individual shot, because the model needs
        # to learn from the specific context of each attempt: where on the floor,
        # how far, what type of move. You can't recover that from game totals.
        #
        # Why store shot_value (2 or 3) explicitly instead of deriving it?
        # Convenience and safety. At query time you'd have to re-parse shot_type
        # every time. Storing it directly makes the model training query simpler
        # and avoids any chance of a parsing bug downstream.
        #
        # Why keep loc_x and loc_y if we already have shot_distance and zone?
        # The zone columns are NBA-defined buckets (coarse). loc_x/loc_y are the
        # raw coordinates in tenths of a foot from the basket. During feature
        # engineering you can derive things like exact angle from basket that the
        # zone columns can't give you. Always store the raw data — you can always
        # bucket it later, but you can't un-bucket it.

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS shots (
            -- shot_id is only unique within a single game (it's a per-game event counter).
            -- The true unique identifier for a shot is (shot_id, game_id) together.
            -- Using shot_id alone as PK caused INSERT OR IGNORE to silently drop shots
            -- from different games that shared the same event counter value.
            shot_id INTEGER NOT NULL,

            game_id TEXT NOT NULL,
            player_id INTEGER NOT NULL,
            team_id INTEGER NOT NULL,

            game_date TEXT NOT NULL,

            period INTEGER NOT NULL,
            minutes_remaining INTEGER NOT NULL,
            seconds_remaining INTEGER NOT NULL,

            shot_made INTEGER NOT NULL,
            shot_value INTEGER NOT NULL,
            shot_distance INTEGER NOT NULL,

            loc_x INTEGER NOT NULL,
            loc_y INTEGER NOT NULL,

            shot_zone_basic TEXT NOT NULL,
            shot_zone_area TEXT NOT NULL,
            shot_zone_range TEXT NOT NULL,

            action_type TEXT NOT NULL,
            shot_type TEXT NOT NULL,

            -- Composite PK: a shot is only truly duplicate if the same event
            -- appears in the same game twice (e.g. pipeline re-run).
            PRIMARY KEY (shot_id, game_id),

            FOREIGN KEY (game_id) REFERENCES games(game_id),
            FOREIGN KEY (player_id) REFERENCES players(player_id),
            FOREIGN KEY (team_id) REFERENCES teams(team_id)
        )
    ''')
        # Index on player_id because the pipeline resumes by checking which
        # players already have shots loaded. Without this index that query
        # does a full table scan, which slows down dramatically as the table grows.
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_shots_player
            ON shots(player_id)
        ''')

        # Index on game_id because the future join to play_by_play (for assist
        # quality) will filter by game_id constantly. Better to build it now.
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_shots_game
            ON shots(game_id)
        ''')

    conn.commit()

if __name__ == "__main__":
    create_tables()
    print("Tables created successfully")
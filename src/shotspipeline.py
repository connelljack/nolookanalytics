import time
import pandas as pd
from nba_api.stats.endpoints import shotchartdetail
from database import get_connection
import traceback


# ---------------------------------------------------------------------------
# Pipeline log — tracks attempted pairs regardless of shot count
# ---------------------------------------------------------------------------

def create_pipeline_log():
    """
    Create a tracking table for completed (player_id, team_id) pairs.

    Why not just check the shots table?
    The shots table only has rows when shots were actually found. A player
    who played but never attempted a field goal (rare but real — think a
    big man who only got fouled, or a DNP who got logged) would have 0 rows
    in shots, so the LEFT JOIN resume check would keep re-fetching them on
    every run. The log table records the *attempt*, not the result, so we
    skip them cleanly next time regardless of how many shots they had.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS shots_pipeline_log (
            player_id INTEGER NOT NULL,
            team_id   INTEGER NOT NULL,
            status    TEXT NOT NULL,  -- 'done' or 'skipped'
            shots_inserted INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (player_id, team_id)
        )
    """)
    conn.commit()
    conn.close()


def get_pending_player_team_pairs():
    """
    Return (player_id, team_id) pairs not yet in the pipeline log.

    This is the key fix — we check the log, not the shots table.
    Any pair that was attempted (whether it had shots or not) is in the log
    and gets skipped. Only genuinely unprocessed pairs come back as pending.
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT DISTINCT ps.player_id, ps.team_id
        FROM player_stats ps
        LEFT JOIN shots_pipeline_log log
            ON ps.player_id = log.player_id
            AND ps.team_id = log.team_id
        WHERE log.player_id IS NULL
        ORDER BY ps.player_id
    """)

    pairs = [(row["player_id"], row["team_id"]) for row in cursor.fetchall()]
    conn.close()
    return pairs


def log_pair(conn, player_id, team_id, status, shots_inserted):
    """
    Write a completed pair to the log so it's skipped on future runs.
    Called after every successful API response — whether shots were found or not.
    """
    conn.cursor().execute("""
        INSERT OR REPLACE INTO shots_pipeline_log (player_id, team_id, status, shots_inserted)
        VALUES (?, ?, ?, ?)
    """, (player_id, team_id, status, shots_inserted))
    conn.commit()


# ---------------------------------------------------------------------------
# Transformation
# ---------------------------------------------------------------------------

def parse_shot_value(shot_type_str):
    """
    Derive the integer point value from the shot_type string.

    The API returns "2PT Field Goal" or "3PT Field Goal". We parse it once
    here at insert time rather than repeatedly at query/model time.

    Returns 2 as the default — if parsing ever fails we'd rather undercount
    a 3 than crash the pipeline.
    """
    if isinstance(shot_type_str, str) and shot_type_str.startswith("3"):
        return 3
    return 2


def transform(df, game_date_lookup):
    """
    Clean and reshape a raw ShotChartDetail dataframe into rows ready for
    insertion.

    We do all transformation here in one place rather than inline in the
    insert loop. This is a good habit — it keeps the insert function focused
    on persistence, not business logic. Easier to test and debug too.

    game_date_lookup: dict of {game_id: game_date} so we can attach the date
    without a DB join on every row.
    """

    # Rename to our schema's conventions — snake_case, no shouting
    df = df.rename(columns={
        "GAME_EVENT_ID":     "shot_id",
        "GAME_ID":           "game_id",
        "PLAYER_ID":         "player_id",
        "TEAM_ID":           "team_id",
        "PERIOD":            "period",
        "MINUTES_REMAINING": "minutes_remaining",
        "SECONDS_REMAINING": "seconds_remaining",
        "SHOT_MADE_FLAG":    "shot_made",
        "SHOT_DISTANCE":     "shot_distance",
        "LOC_X":             "loc_x",
        "LOC_Y":             "loc_y",
        "SHOT_ZONE_BASIC":   "shot_zone_basic",
        "SHOT_ZONE_AREA":    "shot_zone_area",
        "SHOT_ZONE_RANGE":   "shot_zone_range",
        "ACTION_TYPE":       "action_type",
        "SHOT_TYPE":         "shot_type",
    })

    # Derive shot_value from shot_type now, once, for the whole dataframe.
    # apply() runs parse_shot_value on every row's shot_type string.
    df["shot_value"] = df["shot_type"].apply(parse_shot_value)

    # Attach game_date from our lookup dict.
    # .map() is vectorized — it's faster than a Python loop and cleaner than
    # a join. The dict maps game_id -> game_date pulled from the DB earlier.
    df["game_date"] = df["game_id"].map(game_date_lookup)

    # Drop rows where game_date is missing. This would mean we have a shot
    # from a game not in our games table — shouldn't happen but defensive coding.
    df = df.dropna(subset=["game_date"])

    # Select only the columns we're inserting, in the exact order our INSERT
    # statement expects. Being explicit here prevents column order bugs if the
    # API ever changes what it returns.
    columns = [
        "shot_id", "game_id", "player_id", "team_id", "game_date",
        "period", "minutes_remaining", "seconds_remaining",
        "shot_made", "shot_value", "shot_distance",
        "loc_x", "loc_y",
        "shot_zone_basic", "shot_zone_area", "shot_zone_range",
        "action_type", "shot_type"
    ]

    return df[columns]


# ---------------------------------------------------------------------------
# Game date lookup
# ---------------------------------------------------------------------------

def build_game_date_lookup():
    """
    Pull all game_id -> game_date mappings from the DB once upfront.

    Why do this once instead of querying per shot?
    We might insert 200+ shots per player. Hitting the DB once per shot to
    look up a date would be hundreds of unnecessary queries. Pulling the whole
    lookup into memory as a dict is fast and the dict stays small (~1400 entries).
    This is a common pattern called a "lookup table" or "dictionary join".
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT game_id, game_date FROM games")
    lookup = {row["game_id"]: row["game_date"] for row in cursor.fetchall()}
    conn.close()
    return lookup


# ---------------------------------------------------------------------------
# Insert
# ---------------------------------------------------------------------------

def insert_shots(df, conn):
    cursor = conn.cursor()
    for row in df.itertuples(index=False):
        cursor.execute("""
            INSERT OR IGNORE INTO shots (
                shot_id, game_id, player_id, team_id, game_date,
                period, minutes_remaining, seconds_remaining,
                shot_made, shot_value, shot_distance,
                loc_x, loc_y,
                shot_zone_basic, shot_zone_area, shot_zone_range,
                action_type, shot_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (row.shot_id, row.game_id, row.player_id, row.team_id, row.game_date,
            row.period, row.minutes_remaining, row.seconds_remaining,
            row.shot_made, row.shot_value, row.shot_distance,
            row.loc_x, row.loc_y,
            row.shot_zone_basic, row.shot_zone_area, row.shot_zone_range,
            row.action_type, row.shot_type))  # same values, no change here
    conn.commit()
    return len(df)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_shots_pipeline(sleep_between_requests: float = 2.0, max_retries: int = 3):
    """
    Fetch and store all shots for the season.

    Resume behavior: checks shots_pipeline_log for already-attempted pairs
    and skips them — whether they had shots or not. Safe to re-run anytime.

    Args:
        sleep_between_requests: Seconds to wait between API calls. Raised to
            2.0 from 1.2 — the NBA API throttles hard under sustained load
            and the extra second dramatically reduces timeout failures.
        max_retries: How many times to retry a failed request before skipping.
    """
    # Create the log table if it doesn't exist yet. Safe to call every run —
    # CREATE TABLE IF NOT EXISTS is a no-op when the table already exists.
    create_pipeline_log()

    pending = get_pending_player_team_pairs()
    total = len(pending)

    if total == 0:
        print("All shots already loaded. Nothing to do.")
        return

    print(f"Found {total} (player, team) pairs to process...")

    game_date_lookup = build_game_date_lookup()
    conn = get_connection()

    for i, (player_id, team_id) in enumerate(pending, 1):
        for attempt in range(1, max_retries + 1):
            try:
                response = shotchartdetail.ShotChartDetail(
                    player_id= int(player_id),
                    team_id= int(team_id),
                    season_nullable="2025-26",
                    context_measure_simple="FGA",
                    timeout=120   # raised from 60 — gives the API more time to respond
                                  # before we give up and retry. Most timeouts were the
                                  # API being slow, not actually down.
                )

                df_raw = response.get_data_frames()[0]

                if df_raw.empty:
                    # Log the pair as done with 0 shots so we never attempt it again.
                    # Previously we just printed and broke — next run would re-fetch.
                    log_pair(conn, player_id, team_id, status="done", shots_inserted=0)
                    print(f"[{i}/{total}] Player {player_id} / Team {team_id}: no shots, logged and skipping")
                    break

                df_clean = transform(df_raw, game_date_lookup)
                n = insert_shots(df_clean, conn)

                # Log success so this pair is permanently skipped on future runs.
                log_pair(conn, player_id, team_id, status="done", shots_inserted=n)
                print(f"[{i}/{total}] Player {player_id} / Team {team_id}: inserted {n} shots")

                time.sleep(sleep_between_requests)
                break

            except Exception as e:
                import traceback
                traceback.print_exc()
                wait = 5 * attempt
                print(f"[{i}/{total}] Player {player_id} attempt {attempt} failed: {e}. Retrying in {wait}s...")
                time.sleep(wait)
        else:
            # All retries exhausted. Log as skipped so a normal re-run won't
            # retry it — but you can manually delete the log row to force a retry.
            log_pair(conn, player_id, team_id, status="skipped", shots_inserted=0)
            print(f"[{i}/{total}] Player {player_id} / Team {team_id}: SKIPPED after {max_retries} failures")

    conn.close()
    print("Shots pipeline complete!")


if __name__ == "__main__":
    run_shots_pipeline()
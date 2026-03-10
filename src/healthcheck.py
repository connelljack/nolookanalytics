from database import get_connection

def run_health_check():
    conn = get_connection()
    cursor = conn.cursor()

    print("=" * 50)
    print("SHOTS PIPELINE HEALTH CHECK")
    print("=" * 50)

    # Total shots
    cursor.execute("SELECT COUNT(*) as total FROM shots")
    print(f"\nTotal shots: {cursor.fetchone()['total']:,}")

    # Pipeline log summary
    cursor.execute("""
        SELECT status, COUNT(*) as pairs, SUM(shots_inserted) as shots
        FROM shots_pipeline_log
        GROUP BY status
    """)
    print("\nPipeline log:")
    for row in cursor.fetchall():
        print(f"  {row['status']}: {row['pairs']} pairs, {row['shots']:,} shots")

    # Skipped pairs
    cursor.execute("SELECT COUNT(*) as n FROM shots_pipeline_log WHERE status = 'skipped'")
    print(f"\nSkipped pairs (need rerun): {cursor.fetchone()['n']}")

    # Unusual game shot counts
    cursor.execute("""
        SELECT game_id, COUNT(*) as shot_count
        FROM shots
        GROUP BY game_id
        HAVING shot_count < 10 OR shot_count > 250
    """)
    weird_games = cursor.fetchall()
    print(f"\nGames with unusual shot counts (<10 or >250): {len(weird_games)}")
    for g in weird_games[:5]:
        print(f"  {g['game_id']}: {g['shot_count']} shots")

    # Missing game dates
    cursor.execute("SELECT COUNT(*) as n FROM shots WHERE game_date IS NULL")
    print(f"\nShots with missing game_date: {cursor.fetchone()['n']}")

    # Shot value distribution
    cursor.execute("""
        SELECT shot_value, COUNT(*) as n, ROUND(AVG(shot_made) * 100, 1) as fg_pct
        FROM shots
        GROUP BY shot_value
    """)
    print("\nShot value distribution:")
    for row in cursor.fetchall():
        print(f"  {row['shot_value']}PT: {row['n']:,} attempts, {row['fg_pct']}% FG")

    # Date range
    cursor.execute("SELECT MIN(game_date) as earliest, MAX(game_date) as latest FROM shots")
    row = cursor.fetchone()
    print(f"\nDate range: {row['earliest']} → {row['latest']}")

    print("\n" + "=" * 50)
    conn.close()

run_health_check()
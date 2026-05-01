nolookanalytics is a NBA analytics pipeline to ingest game,player, and shot chart data from the NBA API into a local SQlite database.
Project Structure:
db/
  nba.db  #SQLite database that is auto created on first run
src/
  database.py #DB connection + table/schema creation
  pipeline.py #Main ingestion pipeline for teams, games, players, and stats
  shotspipeline.py #Ingestion script for every shot this season
  shotpipelineExplore.py #Exploration/debugging script
  healthcheck.py #DB check
  shot_viz.py #Example code for how data could be visualized 
pyproject.toml
requirements.txt
README.md

NOTE: db/ FOLDER MUST EXIST BEFORE RUNNING. The pipeline writes nba.db there automatically

Prerequisites:
  Python 3.11+
  Either uv (recommended) or pip

Installation

1. git clone https://github.com/connelljack/nolookanalytics.git
   cd nolook analytics

2. mkdir -p db

3. UV or Pip
   curl -LsSf https://astral.sh/uv/install.sh | sh
   uv venv
   source .venv/bin/activate
   uv sync

   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt

Usage
Run scripts from src/

cd src
python database.py  #Creates all tables
Expected output: Tables created successfully

python pipeline.py #Fetches teams,games,players etc for the 25-26 season
Expected Output: 
Fetching data from NBA API...
Inserting teams...
Inserting games...
Inserting team stats...
Fetching player data...
Inserting player stats...
Pipeline complete!

NOTE: THIS CAN TAKE UPWARDS OF 10 MINUTES. THERE IS RETRY LOGIC FOR THE NBA API RATE-LIMITS. IF THAT FAILS YOU CAN QUIT AND RERUN THE SCRIPT AND IT WILL PICKUP WHERE IT LEFT OFF.

if you would like to add shots aswell:
python shotspipeline.py 
Expected output:
Found N (player, team) pairs to process...
[1/N] Player 123456 / Team 1610612738: inserted 312 shots
[2/N] Player 234567 / Team 1610612738: inserted 198 shots
...
Shots pipeline complete!

NOTE: SAME LOGIC AS PIPELINE.PY and RATE LIMITING

python healthcheck.py
==================================================
SHOTS PIPELINE HEALTH CHECK
==================================================

Total shots: 218,432

Pipeline log:
  done: 542 pairs, 218,432 shots

Skipped pairs (need rerun): 0

Games with unusual shot counts (<10 or >250): 2

Shots with missing game_date: 0

Shot value distribution:
  2PT: 134,201 attempts, 53.2% FG
  3PT:  84,231 attempts, 36.8% FG

Date range: 2025-10-22 → 2026-04-13
==================================================

To generate a shot map:
python shot_viz.py
Enter a Player name in the prompt
Output file: src/Jayson_Tatum_shot_map.png


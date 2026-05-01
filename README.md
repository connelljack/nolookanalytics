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

1.

import time
import pandas as pd
from nba_api.stats.endpoints import shotchartdetail
from database import get_connection

response = shotchartdetail.ShotChartDetail(
    player_id=1628369,
    team_id=1610612738,
    season_nullable="2024-25",   # fix — not "2024-2025"
    context_measure_simple="FGA",
    timeout=60
)

df = response.get_data_frames()[0]
print(df.shape)
print(df.columns.tolist())
print(df[['GAME_ID', 'GAME_DATE', 'PERIOD', 'MINUTES_REMAINING', 
          'SECONDS_REMAINING', 'ACTION_TYPE', 'SHOT_TYPE', 
          'SHOT_DISTANCE', 'LOC_X', 'LOC_Y', 
          'SHOT_MADE_FLAG']].head(10).to_string())

# 1. What's the breakdown of made vs missed?
print(df['SHOT_MADE_FLAG'].value_counts())

# 2. How many unique action types are there?
print(df['ACTION_TYPE'].nunique())
print(df['ACTION_TYPE'].value_counts())

# Check the GAME_ID format vs what's in your games table
print(type(df['GAME_ID'].iloc[0]))
print(df['GAME_ID'].iloc[0])

if __name__ == "__main__":
    pass
import sqlite3
import os
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import Circle, Rectangle, Arc
import numpy as np

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'nba.db')

# ── Brand palette ─────────────────────────────────────────────────────────────
BG         = '#F8F7F2'
GREEN_DARK = '#1E6B3C'
GREEN_MID  = '#3A8C5C'
CREAM_DARK = '#D8D4C8'
TEXT       = '#1A1A1A'
SUBTEXT    = '#666666'

HOT_COLORS = {
    'great':  '#1E6B3C',
    'good':   '#3A8C5C',
    'avg':    '#8FBC8F',
    'poor':   '#C8784A',
    'cold':   '#A63C2A',
    'insuff': '#BBBBBB',
}

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# ── Court ─────────────────────────────────────────────────────────────────────

def draw_court(ax, color=CREAM_DARK, lw=1.5):
    elements = [
        Circle((0, 0), radius=7.5, linewidth=lw, color=color, fill=False),
        Rectangle((-30, -7.5), 60, 0, linewidth=lw, color=color),
        Rectangle((-80, -47.5), 160, 190, linewidth=lw, color=color, fill=False),
        Rectangle((-60, -47.5), 120, 190, linewidth=lw, color=color, fill=False),
        Arc((0, 142.5), 120, 120, theta1=0,   theta2=180, linewidth=lw, color=color, fill=False),
        Arc((0, 142.5), 120, 120, theta1=180, theta2=0,   linewidth=lw, color=color, fill=False, linestyle='dashed'),
        Arc((0, 0), 80, 80, theta1=0, theta2=180, linewidth=lw, color=color, fill=False),
        Rectangle((-220, -47.5), 0, 140, linewidth=lw, color=color),
        Rectangle(( 220, -47.5), 0, 140, linewidth=lw, color=color),
        Arc((0, 0), 475, 475, theta1=22, theta2=158, linewidth=lw, color=color, fill=False),
        Arc((0, 422.5), 120, 120, theta1=180, theta2=0, linewidth=lw, color=color, fill=False),
    ]
    for el in elements:
        ax.add_patch(el)
    ax.set_xlim(-260, 260)
    ax.set_ylim(-50, 470)
    ax.set_aspect('equal')
    ax.axis('off')

# ── Data ──────────────────────────────────────────────────────────────────────

def fetch_player(name):
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("SELECT player_id, player_name FROM players WHERE player_name LIKE ?", (f'%{name}%',))
    row  = cur.fetchone()
    conn.close()
    if not row:
        print(f"Player '{name}' not found.")
        return None, None
    return row['player_id'], row['player_name']

def fetch_shots(player_id):
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("""
        SELECT loc_x, loc_y, shot_made, shot_value, shot_zone_basic
        FROM shots WHERE player_id = ?
    """, (player_id,))
    rows = cur.fetchall()
    conn.close()
    return rows

def zone_stats(shots):
    zones = {}
    for s in shots:
        z = s['shot_zone_basic']
        if z not in zones:
            zones[z] = {'made': 0, 'att': 0}
        zones[z]['att']  += 1
        zones[z]['made'] += s['shot_made']
    return {z: {'att': v['att'], 'fg': v['made'] / v['att'] * 100 if v['att'] else 0}
            for z, v in zones.items()}

# ── Zone bubble helpers ───────────────────────────────────────────────────────

ZONE_CENTROIDS = {
    'Restricted Area':       (  0,  30),
    'In The Paint (Non-RA)': (  0, 100),
    'Mid-Range':             (  0, 220),
    'Left Corner 3':         (-210,  20),
    'Right Corner 3':        ( 210,  20),
    'Above the Break 3':     (  0, 320),
    'Backcourt':             (  0, 430),
}

def zone_color(fg, att, league_avg=47.0):
    if att < 10:
        return HOT_COLORS['insuff'], 0.5
    diff = fg - league_avg
    if diff >  8: return HOT_COLORS['great'], 0.90
    if diff >  3: return HOT_COLORS['good'],  0.80
    if diff > -3: return HOT_COLORS['avg'],   0.75
    if diff > -8: return HOT_COLORS['poor'],  0.80
    return HOT_COLORS['cold'], 0.90

# ── Main ──────────────────────────────────────────────────────────────────────

def generate_shot_map(player_name):
    player_id, full_name = fetch_player(player_name)
    if not player_id:
        return

    shots = fetch_shots(player_id)
    if not shots:
        print(f"No shots found for {full_name}.")
        return

    total  = len(shots)
    makes  = sum(s['shot_made'] for s in shots)
    zstats = zone_stats(shots)

    fig, ax = plt.subplots(figsize=(10, 11), facecolor=BG)
    ax.set_facecolor(BG)
    fig.patch.set_facecolor(BG)

    draw_court(ax, color=CREAM_DARK, lw=1.8)

    # Shot scatter
    made_x = [s['loc_x'] for s in shots if     s['shot_made']]
    made_y = [s['loc_y'] for s in shots if     s['shot_made']]
    miss_x = [s['loc_x'] for s in shots if not s['shot_made']]
    miss_y = [s['loc_y'] for s in shots if not s['shot_made']]

    ax.scatter(miss_x, miss_y, c='#C8784A', s=6, alpha=0.18, linewidths=0, zorder=2)
    ax.scatter(made_x, made_y, c=GREEN_MID,  s=6, alpha=0.22, linewidths=0, zorder=2)

    # Fixed size zone bubbles — same size for every zone
    BUBBLE_SIZE = 2800

    for zone, (cx, cy) in ZONE_CENTROIDS.items():
        if zone not in zstats:
            continue
        att = zstats[zone]['att']
        fg  = zstats[zone]['fg']
        col, alpha = zone_color(fg, att)

        ax.scatter(cx, cy, s=BUBBLE_SIZE, c=col, alpha=alpha, zorder=3,
                   edgecolors=BG, linewidths=1.5)

        if att >= 10:
            ax.text(cx, cy + 8,  f"{fg:.0f}%",
                    ha='center', va='center', fontsize=10,
                    fontweight='bold', color='white', zorder=4)
            ax.text(cx, cy - 8, f"{att}",
                    ha='center', va='center', fontsize=8,
                    color='white', alpha=0.9, zorder=4)

    # Legend
    for label, col in [('Elite', HOT_COLORS['great']),
                        ('Above Avg', HOT_COLORS['good']),
                        ('Average', HOT_COLORS['avg']),
                        ('Below Avg', HOT_COLORS['poor']),
                        ('Cold', HOT_COLORS['cold'])]:
        ax.scatter([], [], c=col, s=80, label=label)
    ax.legend(loc='upper right', fontsize=8, facecolor=BG,
              labelcolor=TEXT, edgecolor=CREAM_DARK, framealpha=0.95,
              title='vs LG AVG', title_fontsize=7)

    # Header
    fig.text(0.5, 0.97, full_name.upper(),
             ha='center', va='top', fontsize=22,
             fontweight='bold', color=GREEN_DARK, fontfamily='serif')
    fig.text(0.5, 0.935,
             f'HOT ZONES  ·  {makes}/{total} FG ({makes/total*100:.1f}%)  ·  2024-25',
             ha='center', va='top', fontsize=9,
             color=SUBTEXT, fontfamily='serif')
    fig.text(0.5, 0.01, '@nolookanalytics',
             ha='center', va='bottom', fontsize=8,
             color=SUBTEXT, fontfamily='serif')

    # Save
    safe_name = full_name.replace(' ', '_')
    out_path  = os.path.join(os.path.dirname(__file__), f"{safe_name}_shot_map.png")
    plt.savefig(out_path, dpi=180, bbox_inches='tight',
                facecolor=BG, edgecolor='none')
    plt.close()
    print(f"\nSaved → {out_path}")


if __name__ == "__main__":
    while True:
        name = input("\nEnter player name (or 'quit'): ")
        if name.lower() == 'quit':
            break
        generate_shot_map(name)
import pandas as pd
from pybaseball import statcast
from datetime import datetime
import sqlite3

conn = sqlite3.connect("baseball.db")

# 2015 - 2024 regular season games:
# Beginning: April 05 2015 (2015-04-05)
# Ending: Sept. 30 2024 (2024-09-30)

start_date = datetime(2015, 4, 1)
start_date = start_date.strftime("%Y-%m-%d")

end_date = datetime(2024, 9, 30)
end_date = end_date.strftime("%Y-%m-%d")

data = statcast(start_dt=start_date, end_dt=end_date)

data = data[data["game_type"] == "R"]  # Keep only regular season games

coltype = {
    "pitch_type": "TEXT",
    "game_date": "TEXT",
    "release_speed": "REAL",
    "release_pos_x": "REAL",
    "release_pos_z": "REAL",
    "player_name": "TEXT",
    "batter": "INTEGER",
    "pitcher": "INTEGER",
    "events": "TEXT",
    "description": "TEXT",
    "spin_dir": "INTEGER",
    "spin_rate_deprecated": "INTEGER",
    "break_angle_deprecated": "INTEGER",
    "break_length_deprecated": "INTEGER",
    "zone": "INTEGER",
    "des": "TEXT",
    "game_type": "TEXT",
    "stand": "TEXT",
    "p_throws": "TEXT",
    "home_team": "TEXT",
    "away_team": "TEXT",
    "type": "TEXT",
    "hit_location": "INTEGER",
    "bb_type": "TEXT",
    "balls": "INTEGER",
    "strikes": "INTEGER",
    "game_year": "INTEGER",
    "pfx_x": "REAL",
    "pfx_z": "REAL",
    "plate_x": "REAL",
    "plate_z": "REAL",
    "on_3b": "INTEGER",
    "on_2b": "INTEGER",
    "on_1b": "INTEGER",
    "outs_when_up": "INTEGER",
    "inning": "INTEGER",
    "inning_topbot": "TEXT",
    "hc_x": "REAL",
    "hc_y": "REAL",
    "tfs_deprecated": "INTEGER",
    "tfs_zulu_deprecated": "INTEGER",
    "umpire": "INTEGER",
    "sv_id": "TEXT",
    "vx0": "REAL",
    "vy0": "REAL",
    "vz0": "REAL",
    "ax": "REAL",
    "ay": "REAL",
    "az": "REAL",
    "sz_top": "REAL",
    "sz_bot": "REAL",
    "hit_distance_sc": "INTEGER",
    "launch_speed": "REAL",
    "launch_angle": "INTEGER",
    "effective_speed": "REAL",
    "release_spin_rate": "REAL",
    "release_extension": "REAL",
    "game_pk": "INTEGER",
    "fielder_2": "INTEGER",
    "fielder_3": "INTEGER",
    "fielder_4": "INTEGER",
    "fielder_5": "INTEGER",
    "fielder_6": "INTEGER",
    "fielder_7": "INTEGER",
    "fielder_8": "INTEGER",
    "fielder_9": "INTEGER",
    "release_pos_y": "REAL",
    "estimated_ba_using_speedangle": "REAL",
    "estimated_woba_using_speedangle": "REAL",
    "woba_value": "REAL",
    "woba_denom": "INTEGER",
    "babip_value": "INTEGER",
    "iso_value": "INTEGER",
    "launch_speed_angle": "INTEGER",
    "at_bat_number": "INTEGER",
    "pitch_number": "INTEGER",
    "pitch_name": "TEXT",
    "home_score": "INTEGER",
    "away_score": "INTEGER",
    "bat_score": "INTEGER",
    "fld_score": "INTEGER",
    "post_away_score": "INTEGER",
    "post_home_score": "INTEGER",
    "post_bat_score": "INTEGER",
    "post_fld_score": "INTEGER",
    "if_fielding_alignment": "TEXT",
    "of_fielding_alignment": "TEXT",
    "spin_axis": "INTEGER",
    "delta_home_win_exp": "REAL",
    "delta_run_exp": "REAL",
    "bat_speed": "REAL",
    "swing_length": "REAL",
    "estimated_slg_using_speedangle": "REAL",
    "delta_pitcher_run_exp": "REAL",
    "hyper_speed": "REAL",
    "home_score_diff": "INTEGER",
    "bat_score_diff": "INTEGER",
    "home_win_exp": "REAL",
    "bat_win_exp": "REAL",
    "age_pit_legacy": "INTEGER",
    "age_bat_legacy": "INTEGER",
    "age_pit": "INTEGER",
    "age_bat": "INTEGER",
    "n_thruorder_pitcher": "INTEGER",
    "n_priorpa_thisgame_player_at_bat": "INTEGER",
    "pitcher_days_since_prev_game": "INTEGER",
    "batter_days_since_prev_game": "INTEGER",
    "pitcher_days_until_next_game": "INTEGER",
    "batter_days_until_next_game": "INTEGER",
    "api_break_z_with_gravity": "REAL",
    "api_break_x_arm": "REAL",
    "api_break_x_batter_in": "REAL",
    "arm_angle": "REAL",
}

data.to_sql("pitches", conn, if_exists="replace", index=False, dtype=coltype)

# Close the connection
conn.close()

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


cursor = conn.cursor()

cursor.execute("CREATE INDEX IF NOT EXISTS idx1 ON pitches(game_date)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx2 ON pitches(pitch_type)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx3 ON pitches(game_pk)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx4 ON pitches(at_bat_number)")


# Creating tables for clustering algo

cursor.execute("""CREATE TABLE PITCHES_CLUSTER AS
                  SELECT game_date, game_pk, at_bat_number, pitch_number, pitch_type, pitch_name,
                         COALESCE(release_speed, (SELECT AVG(release_speed) FROM PITCHES)) AS release_speed,
                         COALESCE(release_pos_x, (SELECT AVG(release_pos_x) FROM PITCHES)) AS release_pos_x,
                         COALESCE(release_pos_z, (SELECT AVG(release_pos_z) FROM PITCHES)) AS release_pos_z,
                         COALESCE(zone, (SELECT AVG(zone) FROM PITCHES)) AS zone,
                         COALESCE(pfx_x, (SELECT AVG(pfx_x) FROM PITCHES)) AS pfx_x,
                         COALESCE(pfx_z, (SELECT AVG(pfx_z) FROM PITCHES)) AS pfx_z,
                         COALESCE(plate_x, (SELECT AVG(plate_x) FROM PITCHES)) AS plate_x,
                         COALESCE(plate_z, (SELECT AVG(plate_z) FROM PITCHES)) AS plate_z,
                         COALESCE(vx0, (SELECT AVG(vx0) FROM PITCHES)) AS vx0,
                         COALESCE(vy0, (SELECT AVG(vy0) FROM PITCHES)) AS vy0,
                         COALESCE(vz0, (SELECT AVG(vz0) FROM PITCHES)) AS vz0,
                         COALESCE(ax, (SELECT AVG(ax) FROM PITCHES)) AS ax,
                         COALESCE(ay, (SELECT AVG(ay) FROM PITCHES)) AS ay,
                         COALESCE(az, (SELECT AVG(az) FROM PITCHES)) AS az,
                         COALESCE(effective_speed, (SELECT AVG(effective_speed) FROM PITCHES)) AS effective_speed,
                         COALESCE(release_spin_rate, (SELECT AVG(release_spin_rate) FROM PITCHES)) AS release_spin_rate,
                         COALESCE(release_extension, (SELECT AVG(release_extension) FROM PITCHES)) AS release_extension,
                         COALESCE(release_pos_y, (SELECT AVG(release_pos_y) FROM PITCHES)) AS release_pos_y,
                         COALESCE(api_break_z_with_gravity, (SELECT AVG(api_break_z_with_gravity) FROM PITCHES)) AS api_break_z_with_gravity,
                         COALESCE(api_break_x_arm, (SELECT AVG(api_break_x_arm) FROM PITCHES)) AS api_break_x_arm,
                         COALESCE(api_break_x_batter_in, (SELECT AVG(api_break_x_batter_in) FROM PITCHES)) AS api_break_x_batter_in
                  FROM PITCHES 
                  """)


query = """CREATE TABLE STATS AS
           WITH stats AS (SELECT AVG(release_speed) AS mu_release_speed,
                                 AVG(release_pos_x) AS mu_release_pos_x,
                                 AVG(release_pos_z) AS mu_release_pos_z,
                                 AVG(zone) AS mu_zone,
                                 AVG(pfx_x) AS mu_pfx_x,
                                 AVG(pfx_z) AS mu_pfx_z,
                                 AVG(plate_x) AS mu_plate_x,
                                 AVG(plate_z) AS mu_plate_z,
                                 AVG(vx0) AS mu_vx0,
                                 AVG(vy0) AS mu_vy0,
                                 AVG(vz0) AS mu_vz0,
                                 AVG(ax) AS mu_ax,
                                 AVG(ay) AS mu_ay,
                                 AVG(az) AS mu_az,
                                 AVG(effective_speed) AS mu_effective_speed,
                                 AVG(release_spin_rate) AS mu_release_spin_rate,
                                 AVG(release_extension) AS mu_release_extension,
                                 AVG(release_pos_y) AS mu_release_pos_y,
                                 AVG(api_break_z_with_gravity) AS mu_api_break_z_with_gravity,
                                 AVG(api_break_x_arm) AS mu_api_break_x_arm,
                                 AVG(api_break_x_batter_in) AS mu_api_break_x_batter_in  
                          FROM pitches_cluster)
            
            SELECT mu_release_speed,
                   mu_release_pos_x,
                   mu_release_pos_z,
                   mu_zone,
                   mu_pfx_x,
                   mu_pfx_z,
                   mu_plate_x,
                   mu_plate_z,
                   mu_vx0,
                   mu_vy0,
                   mu_vz0,
                   mu_ax,
                   mu_ay,
                   mu_az,
                   mu_effective_speed,
                   mu_release_spin_rate,
                   mu_release_extension,
                   mu_release_pos_y,
                   mu_api_break_z_with_gravity,
                   mu_api_break_x_arm,
                   mu_api_break_x_batter_in,
                   SQRT(AVG((release_speed - stats.mu_release_speed) * (release_speed - stats.mu_release_speed))) AS sd_release_speed,
                   SQRT(AVG((release_pos_x - stats.mu_release_pos_x) * (release_pos_x - stats.mu_release_pos_x))) AS sd_release_pos_x,
                   SQRT(AVG((release_pos_z - stats.mu_release_pos_z) * (release_pos_z - stats.mu_release_pos_z))) AS sd_release_pos_z,
                   SQRT(AVG((zone - stats.mu_zone) * (zone - stats.mu_zone))) AS sd_zone,
                   SQRT(AVG((pfx_x - stats.mu_pfx_x) * (pfx_x - stats.mu_pfx_x))) AS sd_pfx_x,
                   SQRT(AVG((pfx_z - stats.mu_pfx_z) * (pfx_z - stats.mu_pfx_z))) AS sd_pfx_z,
                   SQRT(AVG((plate_x - stats.mu_plate_x) * (plate_x - stats.mu_plate_x))) AS sd_plate_x,
                   SQRT(AVG((plate_z - stats.mu_plate_z) * (plate_z - stats.mu_plate_z))) AS sd_plate_z,
                   SQRT(AVG((vx0 - stats.mu_vx0) * (vx0 - stats.mu_vx0))) AS sd_vx0,
                   SQRT(AVG((vy0 - stats.mu_vy0) * (vy0 - stats.mu_vy0))) AS sd_vy0,
                   SQRT(AVG((vz0 - stats.mu_vz0) * (vz0 - stats.mu_vz0))) AS sd_vz0,
                   SQRT(AVG((ax - stats.mu_ax) * (ax - stats.mu_ax))) AS sd_ax,
                   SQRT(AVG((ay - stats.mu_ay) * (ay - stats.mu_ay))) AS sd_ay,
                   SQRT(AVG((az - stats.mu_az) * (az - stats.mu_az))) AS sd_az,
                   SQRT(AVG((effective_speed - stats.mu_effective_speed) * (effective_speed - stats.mu_effective_speed))) AS sd_effective_speed,
                   SQRT(AVG((release_spin_rate - stats.mu_release_spin_rate) * (release_spin_rate - stats.mu_release_spin_rate))) AS sd_release_spin_rate,
                   SQRT(AVG((release_extension - stats.mu_release_extension) * (release_extension - stats.mu_release_extension))) AS sd_release_extension,
                   SQRT(AVG((release_pos_y - stats.mu_release_pos_y) * (release_pos_y - stats.mu_release_pos_y))) AS sd_release_pos_y,
                   SQRT(AVG((api_break_z_with_gravity - stats.mu_api_break_z_with_gravity) * (api_break_z_with_gravity - stats.mu_api_break_z_with_gravity))) AS sd_api_break_z_with_gravity,
                   SQRT(AVG((api_break_x_arm - stats.mu_api_break_x_arm) * (api_break_x_arm - stats.mu_api_break_x_arm))) AS sd_api_break_x_arm,
                   SQRT(AVG((api_break_x_batter_in - stats.mu_api_break_x_batter_in) * (api_break_x_batter_in - stats.mu_api_break_x_batter_in))) AS sd_api_break_x_batter_in
            FROM pitches_cluster, stats;"""

cursor.execute(query)


cursor.execute("""CREATE TABLE PITCHES_CLUSTER_SD AS
                  SELECT game_date, game_pk, at_bat_number, pitch_number, pitch_type, pitch_name,
                         release_speed,
                         (release_speed - mu_release_speed) / sd_release_speed AS z_release_speed,

                         release_pos_x,
                         (release_pos_x - mu_release_pos_x) / sd_release_pos_x AS z_release_pos_x,
 
                         release_pos_z,
                         (release_pos_z - mu_release_pos_z) / sd_release_pos_z AS z_release_pos_z,
 
                         zone,
                         (zone - mu_zone) / sd_zone AS z_zone,
 
                         pfx_x,
                         (pfx_x - mu_pfx_x) / sd_pfx_x AS z_pfx_x,
 
                         pfx_z,
                         (pfx_z - mu_pfx_z) / sd_pfx_z AS z_pfx_z,
 
                         plate_x,
                         (plate_x - mu_plate_x) / sd_plate_x AS z_plate_x,
 
                         plate_z,
                         (plate_z - mu_plate_z) / sd_plate_z AS z_plate_z,
 
                         vx0,
                         (vx0 - mu_vx0) / sd_vx0 AS z_vx0,
 
                         vy0,
                         (vy0 - mu_vy0) / sd_vy0 AS z_vy0,
 
                         vz0,
                         (vz0 - mu_vz0) / sd_vz0 AS z_vz0,
 
                         ax,
                         (ax - mu_ax) / sd_ax AS z_ax,
 
                         ay,
                         (ay - mu_ay) / sd_ay AS z_ay,
 
                         az,
                         (az - mu_az) / sd_az AS z_az,
 
                         effective_speed,
                         (effective_speed - mu_effective_speed) / sd_effective_speed AS z_effective_speed,
 
                         release_spin_rate,
                         (release_spin_rate - mu_release_spin_rate) / sd_release_spin_rate AS z_release_spin_rate,
 
                         release_extension,
                         (release_extension - mu_release_extension) / sd_release_extension AS z_release_extension,
 
                         release_pos_y,
                         (release_pos_y - mu_release_pos_y) / sd_release_pos_y AS z_release_pos_y,
 
                         api_break_z_with_gravity,
                         (api_break_z_with_gravity - mu_api_break_z_with_gravity) / sd_api_break_z_with_gravity AS z_api_break_z_with_gravity,
 
                         api_break_x_arm,
                         (api_break_x_arm - mu_api_break_x_arm) / sd_api_break_x_arm AS z_api_break_x_arm,
 
                         api_break_x_batter_in,
                         (api_break_x_batter_in - mu_api_break_x_batter_in) / sd_api_break_x_batter_in AS z_api_break_x_batter_in

                  from pitches_cluster a
                  left join stats b on 1=1; """)


cursor.execute("CREATE INDEX IF NOT EXISTS idxb1 ON PITCHES_CLUSTER_SD(game_date)")
cursor.execute("CREATE INDEX IF NOT EXISTS idxb2 ON PITCHES_CLUSTER_SD(pitch_type)")
cursor.execute("CREATE INDEX IF NOT EXISTS idxb3 ON PITCHES_CLUSTER_SD(game_pk)")
cursor.execute("CREATE INDEX IF NOT EXISTS idxb4 ON PITCHES_CLUSTER_SD(at_bat_number)")
cursor.execute("DROP TABLE pitches_cluster")


conn.commit()


# Close the connection
conn.close()

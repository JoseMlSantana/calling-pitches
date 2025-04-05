import pandas as pd


def get_pitcher_team(row):
    if row["inning_topbot"] == "Top":
        return row["home_team"]
    elif row["inning_topbot"] == "Bot":
        return row["away_team"]
    else:
        return None


def delta_win_exp_adj(row):
    """Returns delta win prob from pitcher's team perspective"""
    if get_pitcher_team(row) == row["home_team"]:
        return row["delta_home_win_exp"]
    else:
        return -row["delta_home_win_exp"]


def bin_inning(inning):
    if inning <= 3:
        return "early"
    elif inning <= 6:
        return "mid"
    else:
        return "late"


def bin_score(diff):
    if diff <= -4:
        return "trailing"
    elif diff >= 4:
        return "leading"
    else:
        return "close"


def group_event(event):
    if event in ["double", "triple"]:
        return "extra_bases"
    elif event in ["walk", "hit_by_pitch", "intent_walk"]:
        return "walks"
    elif event in [
        "grounded_into_double_play",
        "double_play",
        "sac_fly_double_play",
        "sac_bunt_double_play",
        "strikeout_double_play",
        "triple_play",
    ]:
        return "multiple_outs"
    elif event in ["force_out", "fielders_choice", "fielders_choice_out"]:
        return "force_out"
    elif event in ["sac_fly", "sac_bunt"]:
        return "sacrifice"
    else:
        return event


def bin_count(balls, strikes):
    if balls == 3 and strikes == 2:
        return "Full count"
    if balls >= 3:
        return "hitter_friendly"
    elif strikes >= 2:
        return "pitcher_friendly"
    elif balls == 2 and strikes == 1:
        return "neutral"
    else:
        return "early_count"


def runner_on(row, base_column):
    return 1 if pd.notna(row[base_column]) else 0


def bin_base_state(row):
    runners_on = row["1b"] + row["2b"] + row["3b"]
    risp = row["2b"] + row["3b"]
    if runners_on == 0:
        return "empty"
    if runners_on == 3:
        return "bases_loaded"
    elif risp >= 1:
        return "risp"
    else:
        return "Runner 1b"

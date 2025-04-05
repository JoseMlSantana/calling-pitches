import pandas as pd
from src.features import (
    bin_inning,
    bin_score,
    group_event,
    bin_count,
    bin_base_state,
    delta_win_exp_adj,
)


def generate_utility_table(final_pitches: pd.DataFrame) -> pd.DataFrame:
    """
    Generate a contextual utility table using adjusted win probability.

    Parameters:
    - final_pitches: DataFrame with one row per final pitch of each at-bat.
    - min_count: Minimum number of samples required per group to keep the row.

    Returns:
    - utility_table: DataFrame with avg_utility by situation + event group.
    """

    # Create binary base occupancy columns if not already present
    for base_col, short_col in zip(["on_1b", "on_2b", "on_3b"], ["1b", "2b", "3b"]):
        if short_col not in final_pitches.columns:
            final_pitches[short_col] = final_pitches[base_col].notna().astype(int)

    final_pitches["inning_bin"] = final_pitches["inning"].apply(bin_inning)
    final_pitches["score_bin"] = final_pitches["bat_score_diff"].apply(bin_score)
    final_pitches["count_bin"] = final_pitches.apply(
        lambda row: bin_count(row["balls"], row["strikes"]), axis=1
    )
    final_pitches["event_group"] = final_pitches["events"].apply(group_event)
    final_pitches["base_group"] = final_pitches.apply(bin_base_state, axis=1)
    final_pitches["adjusted_win_delta"] = final_pitches.apply(delta_win_exp_adj, axis=1)

    # Define group keys
    group_keys = [
        "event_group",
        "base_group",
        "outs_when_up",
        "inning_bin",
        "count_bin",
        "score_bin",
    ]

    utility_table = (
        final_pitches.groupby(group_keys)["adjusted_win_delta"]
        .agg(["mean", "count"])
        .reset_index()
        .rename(columns={"mean": "avg_utility"})
    )

    return utility_table

import os
import argparse
from utils import json_handling
from datetime import date

import re


def serialize(data_bucket):
    serializable_play_detail = []
    for (b_id, b_name), stats in data_bucket.items():
        item = {"batter_id": b_id, "batter_name": b_name}
        item.update(stats)
        serializable_play_detail.append(item)

    return serializable_play_detail


def check_event_type(event_type, pattern):
    if event_type is None:
        return False
    return bool(re.match(pattern, event_type, flags=re.IGNORECASE))


def get_default_bucket():

    default_bucket = {
        "at_bat": 0,
        "single": 0,
        "double": 0,
        "triple": 0,
        "home_run": 0,
        "walk": 0,
        "intent_walk": 0,
        "hit_by_pitch": 0,
        "strikeout": 0,
        "field_out": 0,
        "double_play": 0,
        "field_choice": 0,
        "field_choice_out": 0,
        "force_out": 0,
        "triple_play": 0,
        "reach_by_error": 0,
        "sac_fly": 0,
        "sac_bunt": 0,
        "stolen_base": 0,
        "caught_stealing": 0,
        "pickoff_caught": 0,
        "other": 0,
        "ejection": 0,
    }

    return default_bucket.copy()


def get_game_ids(filename):
    game_ids = json_handling.read_raw_json(filename)

    return game_ids


def get_batter_plays(game_ids, raw_dir):

    rows=[]
    for game_id in game_ids:

        detail_file = os.path.join(raw_dir, f"gamedetail_{game_id}.json")
        detail_obj = json_handling.read_raw_json(detail_file)
        play_obj = detail_obj["allPlays"]

        num_plays = len(play_obj)

        data_bucket = {}
        for i in range(num_plays):
            batter_id = play_obj[i]["matchup"]['batter']['id']
            batter_name = play_obj[i]["matchup"]['batter']['fullName']

            bucket_key = (batter_id, batter_name)

            if bucket_key not in data_bucket:
                data_bucket[bucket_key] = get_default_bucket()

            data_bucket[bucket_key]["at_bat"] += 1
            event_type = play_obj[i]["result"]['eventType']

            if event_type == "single":
                data_bucket[bucket_key]["single"] += 1

            elif event_type == "double":
                data_bucket[bucket_key]["double"] += 1

            elif event_type == "triple":
                data_bucket[bucket_key]["triple"] += 1

            elif event_type == "home_run":
                data_bucket[bucket_key]["home_run"] += 1

            elif event_type == "field_out":
                data_bucket[bucket_key]["field_out"] += 1

            elif event_type == "fielders_choice":
                data_bucket[bucket_key]["field_choice"] += 1

            elif event_type == "fielders_choice_out":
                data_bucket[bucket_key]["field_choice_out"] += 1

            elif event_type == "force_out":
                data_bucket[bucket_key]["force_out"] += 1

            elif event_type == "triple_play":
                data_bucket[bucket_key]["triple_play"] += 1

            elif event_type == "walk":
                data_bucket[bucket_key]["walk"] += 1

            elif event_type == "intent_walk":
                data_bucket[bucket_key]["intent_walk"] += 1

            elif event_type == "hit_by_pitch":
                data_bucket[bucket_key]["hit_by_pitch"] += 1

            elif event_type == "field_error":
                data_bucket[bucket_key]["reach_by_error"] += 1

            elif event_type == "ejection":
                data_bucket[bucket_key]["ejection"] += 1

            elif check_event_type(event_type, r"^strike"):
                data_bucket[bucket_key]["strikeout"] += 1

            elif check_event_type(event_type, r"^(double_play|grounded_into_d)"):
                data_bucket[bucket_key]["double_play"] += 1

            elif check_event_type(event_type, r"^sac_f"):
                data_bucket[bucket_key]["sac_fly"] += 1

            elif check_event_type(event_type, r"^sac_b"):
                data_bucket[bucket_key]["sac_bunt"] += 1

            elif check_event_type(event_type, r"^stolen_b"):
                data_bucket[bucket_key]["stolen_base"] += 1

            elif check_event_type(event_type, r"^caught_steal"):
                data_bucket[bucket_key]["caught_stealing"] += 1

            elif check_event_type(event_type, r"^pickoff_caught"):
                data_bucket[bucket_key]["pickoff_caught"] += 1

            else:
                data_bucket[bucket_key]["other"] += 1

        play_detail = serialize(data_bucket)

        rows.append({
            "game_id": game_id,
            "play_detail": play_detail,
        })

    return rows


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("date", type=date.fromisoformat, help="Date to extract (YYYY-MM-DD)")
    parser.add_argument("raw_detail_dir", type=str, help="Directory of raw game details")
    parser.add_argument("clean_detail_dir", type=str, help="Directory of clean game details")

    args = parser.parse_args()

    id_file = os.path.join(args.raw_detail_dir, f"game_ids_{args.date}.json")

    game_ids = get_game_ids(id_file)
    print(f"Found {len(game_ids)} final games: {game_ids}")

    play_rows = get_batter_plays(game_ids, args.raw_detail_dir)
    print("play_rows got!")

    plays_file = os.path.join(args.clean_detail_dir, f"plays_{args.date}.jsonl")
    json_handling.save_clean_jsonl(play_rows, plays_file)
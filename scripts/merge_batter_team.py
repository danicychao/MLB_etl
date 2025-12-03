import os
import argparse
from utils import json_handling, url_request
from datetime import date


def get_player_team(game_id):
    boxscore = url_request.get_boxscore(game_id)

    home = boxscore['teams']['home']
    away = boxscore['teams']['away']

    home_team_id = home['team']['id']
    away_team_id = away['team']['id']

    home_players = home['players']
    away_players = away['players']

    home_player_ids = [int(key[2:]) for key in home_players.keys()]
    away_player_ids = [int(key[2:]) for key in away_players.keys()]

    team_player_ids = {
        "home_team_id": home_team_id,
        "home_player_ids": home_player_ids,
        "away_team_id": away_team_id,
        "away_player_ids": away_player_ids,
    }

    return team_player_ids


def get_team_id(player_id, team_player_ids):
    if player_id in team_player_ids["home_player_ids"]:
        return team_player_ids["home_team_id"]

    if player_id in team_player_ids["away_player_ids"]:
        return team_player_ids["away_team_id"]

    return None


def make_detail_lines(rows, game_date):
    str_date = game_date.isoformat()
    season = str_date[:4]

    detail_lines = []
    num_games = len(rows)

    for i in range(num_games):
        game_id = rows[i]['game_id']
        team_player_ids = get_player_team(game_id)

        play_details = rows[i]['play_detail']
        num_batters = len(play_details)
        for j in range(num_batters):

            batter_id = play_details[j]['batter_id']

            detail_lines.append({
                "game_id": game_id,
                "game_date": str_date,
                "season": season,
                "batter_id": batter_id,
                "team_id": get_team_id(batter_id, team_player_ids),
                "batter_name": play_details[j]['batter_name'],

                "at_bat": play_details[j]['at_bat'],
                "single": play_details[j]['single'],
                "double": play_details[j]['double'],
                "triple": play_details[j]['triple'],
                "home_run": play_details[j]['home_run'],
                "walk": play_details[j]['walk'],
                "intent_walk": play_details[j]['intent_walk'],
                "hit_by_pitch": play_details[j]['hit_by_pitch'],
                "strikeout": play_details[j]['strikeout'],
                "field_out": play_details[j]['field_out'],
                "double_play": play_details[j]['double_play'],
                "field_choice": play_details[j]['field_choice'],
                "field_choice_out": play_details[j]['field_choice_out'],
                "force_out": play_details[j]['force_out'],
                "triple_play": play_details[j]['triple_play'],
                "reach_by_error": play_details[j]['reach_by_error'],
                "sac_fly": play_details[j]['sac_fly'],
                "sac_bunt": play_details[j]['sac_bunt'],
                "stolen_base": play_details[j]['stolen_base'],
                "caught_stealing": play_details[j]['caught_stealing'],
                "pickoff_caught": play_details[j]['pickoff_caught'],
                "other": play_details[j]['other'],
                "ejection": play_details[j]['ejection'],
            })
    return detail_lines



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("date", type=date.fromisoformat, help="Game date (YYYY-MM-DD)")
    parser.add_argument("clean_detail_dir", type=str, help="Directory of clean play details")

    args = parser.parse_args()

    input_clean_file = os.path.join(args.clean_detail_dir, f"plays_{args.date}.jsonl")
    rows = json_handling.read_jsonl(input_clean_file)
    print(f"Read {len(rows)} rows from {input_clean_file}")

    detail_lines = make_detail_lines(rows, args.date)
    print(f"Get {len(detail_lines)} plays on {args.date}")

    gamedetail_clean_file = os.path.join(args.clean_detail_dir, f"gamedetails_{args.date}_clean.jsonl")
    json_handling.save_clean_jsonl(detail_lines, gamedetail_clean_file)

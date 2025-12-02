import os
import argparse
from utils import url_request, json_handling
from datetime import date


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("date", type=date.fromisoformat, help="Date to extract (YYYY-MM-DD)")
    parser.add_argument("out_dir", type=str, help="Raw data directory")

    args = parser.parse_args()

    games = url_request.get_games(args.date)
    game_file = os.path.join(args.out_dir, f"games_{args.date}.json")
    json_handling.save_json(games, game_file)
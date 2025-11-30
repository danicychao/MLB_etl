import requests
import os
import json
import argparse
from datetime import date


BASE = "https://statsapi.mlb.com/api/v1"


def get_games(date):
    url = f"{BASE}/schedule?sportId=1&date={date}"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.json()


def save_raw(obj, filename):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("date", type=date.fromisoformat, help="Date to extract (YYYY-MM-DD)")
    parser.add_argument("out_dir", type=str, help="Raw data directory")

    args = parser.parse_args()

    games = get_games(args.date)
    game_file = os.path.join(args.out_dir, f"games_{args.date}.json")
    save_raw(games, game_file)
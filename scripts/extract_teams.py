import requests
import os
import json
import argparse


BASE = "https://statsapi.mlb.com/api/v1"


def get_teams(year):
    url = f"{BASE}/teams?sportId=1&season={year}"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.json()


def save_raw(obj, filename):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, help="Year to extract")
    parser.add_argument("out_dir", type=str, help="Raw data directory")

    args = parser.parse_args()

    teams = get_teams(args.year)
    team_file = os.path.join(args.out_dir, f"teams_{args.year}.json")
    save_raw(teams, team_file)
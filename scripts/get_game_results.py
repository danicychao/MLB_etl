import os
import json
import argparse
from datetime import date


def get_results_from_file(infile):
    with open(infile, encoding="utf-8") as fh:
        j = json.load(fh)

    data = j.get("dates")[0]
    num_games = j.get("totalGames")

    rows = []
    for i in range(num_games):
        if data["games"][i]["gameType"] != "R" or data["games"][i]["status"]["abstractGameState"] != "Final":
            continue

        season = data["games"][i]["season"]
        game_id = data["games"][i]["gamePk"]
        game_date = data["games"][i]["officialDate"]
        game_status = data["games"][i]["status"]["abstractGameState"]

        home_team = data["games"][i]["teams"]["home"]["team"]["id"]
        away_team = data["games"][i]["teams"]["away"]["team"]["id"]

        home_score = data["games"][i]["teams"]["home"]["score"]
        away_score = data["games"][i]["teams"]["away"]["score"]

        home_team_win = 1 if data["games"][i]["teams"]["home"]["isWinner"] == True else 0

        rows.append({
            "game_id": game_id,
            "game_date": game_date,
            "game_status": game_status,
            "home_team": home_team,
            "away_team": away_team,
            "home_score": home_score,
            "away_score": away_score,
            "home_team_win": home_team_win,
            "season": season,
        })

    return rows

def save_clean_jsonl(rows, filename):
    with open(filename, "w", encoding="utf-8") as f:
        for r in rows:
            json.dump(r, f)
            f.write("\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("date", type=date.fromisoformat, help="Date to extract (YYYY-MM-DD)")
    parser.add_argument("in_dir", type=str, help="Raw data directory")
    parser.add_argument("out_dir", type=str, help="Clean data directory")

    args = parser.parse_args()

    raw_file = os.path.join(args.in_dir, f"games_{args.date}.json")
    clean_file = os.path.join(args.out_dir, f"games_{args.date}_clean.jsonl")

    rows = get_results_from_file(raw_file)
    save_clean_jsonl(rows, clean_file)

    print(f"Write {len(rows)} cleaned rows to {clean_file}")



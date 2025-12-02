import os
import argparse
from utils import json_handling, url_request
from datetime import date


def get_game_id(filename):
    if not os.path.exists(filename):
        print(f"Warning: File not found: {filename}")
        return []

    rows = json_handling.read_jsonl(filename)
    if not rows:
        print(f"Warning: Empty file: {filename}")
        return []

    game_ids = [r["game_id"] for r in rows if r.get("game_status") == "Final"]
    return game_ids


def download_play_by_play(game_ids, detail_dir):
    for game_id in game_ids:
        detail_file = os.path.join(detail_dir, f"gamedetail_{game_id}.json")
        game = url_request.get_play_by_play(game_id)
        json_handling.save_json(game, detail_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("date", type=date.fromisoformat, help="Date to extract (YYYY-MM-DD)")
    parser.add_argument("clean_dir", type=str, help="Directory with clean game results")
    parser.add_argument("detail_dir", type=str, help="Directory of game details")

    args = parser.parse_args()

    results_file = os.path.join(args.clean_dir, f"games_{args.date}_clean.jsonl")

    game_ids = get_game_id(results_file)
    print(f"Found {len(game_ids)} final games: {game_ids}")

    ids_file = os.path.join(args.detail_dir, f"game_ids_{args.date}.json")
    json_handling.save_json(game_ids, ids_file)

    download_play_by_play(game_ids, args.detail_dir)
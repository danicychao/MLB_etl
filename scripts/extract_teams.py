import os
import argparse


from utils import url_request
from utils import json_handling


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, help="Year to extract")
    parser.add_argument("out_dir", type=str, help="Raw data directory")

    args = parser.parse_args()

    teams = url_request.get_teams(args.year)
    team_file = os.path.join(args.out_dir, f"teams_{args.year}.json")
    json_handling.save_json(teams, team_file)
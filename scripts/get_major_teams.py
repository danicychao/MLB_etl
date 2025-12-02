import os
import json
import argparse


from utils import json_handling


def get_teams_from_file(infile):
    with open(infile, encoding="utf-8") as fh:
        j = json.load(fh)

    teams = j.get("teams")

    rows = []
    for t in teams:
        team_id = t.get("id")

        league = t.get("league")
        league_id = league.get("id")
    
        division = t.get("division")
        division_id = division.get("id")

        team_name = t.get("teamName")
        abbr = t.get("abbreviation")

        first_year = int(t.get("firstYearOfPlay"))
        season = int(t.get("season"))

        city = t.get("locationName")
        
        venue = t.get("venue")
        venue_name = venue.get("name")

        rows.append({
            "season": season,
            "id": team_id,
            "league_id": league_id,
            "division_id": division_id,
            "team_name": team_name,
            "abbreviation": abbr,
            "city": city,
            "ball_park": venue_name,
            "first_year": first_year,
        })

    return rows


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, help="Year to extract")
    parser.add_argument("in_dir", type=str, help="Raw data directory")
    parser.add_argument("out_dir", type=str, help="Clean data directory")

    args = parser.parse_args()

    raw_file = os.path.join(args.in_dir, f"teams_{args.year}.json")
    clean_file = os.path.join(args.out_dir, f"teams_{args.year}_clean.jsonl") 

    rows = get_teams_from_file(raw_file)
    json_handling.save_clean_jsonl(rows, clean_file)

    
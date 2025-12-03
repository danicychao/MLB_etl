import os
import argparse
from utils import json_handling
from datetime import date

import psycopg2
from psycopg2.extras import execute_values

DB_HOST = os.getenv("DATA_DB_HOST", "localhost")
DB_NAME = os.getenv("DATA_DB_NAME", "mlb_db")
DB_USER = os.getenv("DATA_DB_USER", "mlb_user")
DB_PASS = os.getenv("DATA_DB_PASS", "mlb_pass")
DB_PORT = int(os.getenv("DATA_DB_PORT", 5432))


def upsert_rows(conn, rows):
    if not rows:
        print("No rows to insert.")
        return

    values = [
        (
            r.get("game_id"),
            r.get("game_date"),
            r.get("game_status"),
            r.get("home_team"),
            r.get("away_team"),
            r.get("home_score"),
            r.get("away_score"),
            r.get("home_team_win"),
            int(r.get("season")),
        )
        for r in rows
    ]

    sql = """
    INSERT INTO game (
        game_id, game_date, game_status, 
        home_team_id, away_team_id, score_home, score_away, home_team_win, season)
    VALUES %s
    
    ON CONFLICT (game_id) DO UPDATE SET
        game_date = EXCLUDED.game_date,
        game_status = EXCLUDED.game_status,
        home_team_id = EXCLUDED.home_team_id,
        away_team_id = EXCLUDED.away_team_id,
        score_home = EXCLUDED.score_home,
        score_away = EXCLUDED.score_away,
        home_team_win = EXCLUDED.home_team_win,
        season = EXCLUDED.season;
    """

    cur = conn.cursor()
    execute_values(cur, sql, values)
    conn.commit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("date", type=date.fromisoformat, help="Game date (YYYY-MM-DD)")
    parser.add_argument("in_dir", type=str, help="Clean data directory")

    args = parser.parse_args()

    input_clean_file = os.path.join(args.in_dir, f"games_{args.date}_clean.jsonl")
    rows = json_handling.read_jsonl(input_clean_file)
    print(f"Read {len(rows)} rows from {input_clean_file}")

    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT
    )

    # create_table(conn)
    upsert_rows(conn, rows)
    conn.close()
    print("Done loading to Postgres.")

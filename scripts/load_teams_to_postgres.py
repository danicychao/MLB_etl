import os

import argparse
from utils import json_handling

import psycopg2
from psycopg2.extras import execute_values


DB_HOST = os.getenv("DATA_DB_HOST", "localhost")
DB_NAME = os.getenv("DATA_DB_NAME", "mlb_db")
DB_USER = os.getenv("DATA_DB_USER", "mlb_user")
DB_PASS = os.getenv("DATA_DB_PASS", "mlb_pass")
DB_PORT = int(os.getenv("DATA_DB_PORT", 5432))


def create_table(conn):
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS teams (
        season INTEGER,
        id INTEGER,
        league_id INTEGER,
        division_id INTEGER,
        team_name TEXT,
        city TEXT,
        abbreviation TEXT,
        ball_park TEXT,
        first_year INTEGER,
        PRIMARY KEY (season, id)
    );
    """)
    conn.commit()


def upsert_rows(conn, rows):
    if not rows:
        print("No rows to insert.")
        return

    values = [
        (
            r.get("season"),
            r.get("id"),
            r.get("league_id"),
            r.get("division_id"),
            r.get("team_name"),
            r.get("city"),
            r.get("abbreviation"),
            r.get("ball_park"),
            r.get("first_year"),
        )
        for r in rows
    ]

    sql = """
    INSERT INTO teams (season, id, league_id, division_id, team_name, city, abbreviation, ball_park, first_year)
    VALUES %s
    """

    cur = conn.cursor()
    execute_values(cur, sql, values)
    conn.commit()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, help="Year to extract")
    parser.add_argument("in_dir", type=str, help="Clean data directory")

    args = parser.parse_args()

    input_clean_file = os.path.join(args.in_dir, f"teams_{args.year}_clean.jsonl") 
    rows = json_handling.read_jsonl(input_clean_file)

    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT
    )

    # create_table(conn)
    upsert_rows(conn, rows)
    conn.close()
    print("Done loading to Postgres.")


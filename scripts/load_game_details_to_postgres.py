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
            r.get("team_id"),
            r.get("batter_id"),
            r.get("batter_name"),

            r.get("at_bat"),
            r.get("single"),
            r.get("double"),
            r.get("triple"),
            r.get("home_run"),

            r.get("walk"),
            r.get("intent_walk"),
            r.get("hit_by_pitch"),

            r.get("strikeout"),
            r.get("field_out"),
            r.get("double_play"),
            r.get("field_choice"),
            r.get("field_choice_out"),
            r.get("force_out"),
            r.get("triple_play"),

            r.get("reach_by_error"),
            r.get("sac_fly"),
            r.get("sac_bunt"),

            r.get("stolen_base"),
            r.get("caught_stealing"),
            r.get("pickoff_caught"),
            r.get("other"),
            r.get("ejection"),
            int(r.get("season")),
        )
        for r in rows
    ]

    sql = """
    INSERT INTO game_details_batter (
        game_id, game_date, team_id, 
        batter_id, batter_name, at_bat, 
        single, double, triple, home_run,
        walk, intent_walk, hit_by_pitch, strikeout,
        field_out, double_play, field_choice, field_choice_out,
        force_out, triple_play, reach_by_error, sac_fly, sac_bunt,
        stolen_base, caught_stealing, pickoff_caught,
        other, ejection, season)
    VALUES %s

    ON CONFLICT (batter_id, game_id) DO UPDATE SET
        game_date = EXCLUDED.game_date,
        team_id = EXCLUDED.team_id,
        batter_name = EXCLUDED.batter_name,
        at_bat = EXCLUDED.at_bat,
        single = EXCLUDED.single,
        double = EXCLUDED.double,
        triple = EXCLUDED.triple,
        home_run = EXCLUDED.home_run,
        walk = EXCLUDED.walk,
        intent_walk = EXCLUDED.intent_walk,
        hit_by_pitch = EXCLUDED.hit_by_pitch,
        strikeout = EXCLUDED.strikeout,
        field_out = EXCLUDED.field_out,
        double_play = EXCLUDED.double_play,
        field_choice = EXCLUDED.field_choice,
        field_choice_out = EXCLUDED.field_choice_out,
        force_out = EXCLUDED.force_out,
        triple_play = EXCLUDED.triple_play,
        reach_by_error = EXCLUDED.reach_by_error,
        sac_fly = EXCLUDED.sac_fly,
        sac_bunt = EXCLUDED.sac_bunt,
        stolen_base = EXCLUDED.stolen_base,
        caught_stealing = EXCLUDED.caught_stealing,
        pickoff_caught = EXCLUDED.pickoff_caught,
        other = EXCLUDED.other,
        ejection = EXCLUDED.ejection,
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

    input_clean_file = os.path.join(args.in_dir, f"gamedetails_{args.date}_clean.jsonl")
    rows = json_handling.read_jsonl(input_clean_file)
    print(f"Read {len(rows)} rows from {input_clean_file}")

    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT
    )

    upsert_rows(conn, rows)
    conn.close()
    print("Done loading to Postgres.")

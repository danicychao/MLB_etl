import os
import json

def save_json(obj, filename):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

    print(f"Write raw data into {filename}")


def save_clean_jsonl(rows, filename):
    with open(filename, "w", encoding="utf-8") as f:
        for r in rows:
            json.dump(r, f)
            f.write("\n")

    print(f"Write {len(rows)} cleaned rows to {filename}")


def read_raw_json(filename):
    if not os.path.exists(filename):
        print("No input file:", filename)
        return None

    with open(filename, encoding="utf-8") as fh:
        obj = json.load(fh)

    return obj


def read_jsonl(filename):
    if not os.path.exists(filename):
        print("No input file:", filename)
        return []

    rows = []
    with open(filename, "r", encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                rows.append(json.loads(line))

    return rows
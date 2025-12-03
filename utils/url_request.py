import requests


BASE = "https://statsapi.mlb.com/api/v1"

def get_teams(year):
    url = f"{BASE}/teams?sportId=1&season={year}"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.json()


def get_games(date):
    url = f"{BASE}/schedule?sportId=1&date={date}"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.json()


def get_play_by_play(game_id):
    url = f"{BASE}/game/{game_id}/playByPlay"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.json()


def get_boxscore(game_id):
    url = f"{BASE}/game/{game_id}/boxscore"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.json()
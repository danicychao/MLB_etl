CREATE TABLE game (
	game_id INTEGER,
	game_date DATE,
	game_status TEXT,
	home_team_id INTEGER,
	away_team_id INTEGER,
	score_home INTEGER,
	score_away INTEGER,
	home_team_win INTEGER,
	PRIMARY KEY (game_id)
)
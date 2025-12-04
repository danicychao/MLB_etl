WITH home_run_leaders AS (
	SELECT
	max(batter_name) AS batter_name,
	sum(home_run) AS home_runs,
	max(team_id) AS team_id
	FROM game_details_batter
	GROUP BY batter_id
	ORDER BY home_runs DESC
	LIMIT 10)

SELECT
t.team_name as team_name,
hr.batter_name as batter_name,
hr.home_runs as home_runs
FROM home_run_leaders as hr LEFT JOIN teams as t
ON CAST(hr.team_id AS INTEGER) = t.id
WHERE t.season = 2025
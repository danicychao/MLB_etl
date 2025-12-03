SELECT
max(batter_name) AS batter_name,
sum(home_run) AS home_runs
FROM game_details_batter
GROUP BY batter_id
ORDER BY home_runs DESC
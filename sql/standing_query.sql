WITH home_results AS (
SELECT
home_team_id as team_id,
home_team_win as win,
CASE WHEN home_team_win = 1 THEN 0
	 ELSE 1
	 END AS loss
FROM game
),

away_results AS (
SELECT
away_team_id as team_id,
home_team_win as loss,
CASE WHEN home_team_win = 1 THEN 0
	 ELSE 1
	 END AS win
FROM game
),

overall_results AS (
SELECT * FROM home_results
UNION ALL
SELECT * FROM away_results
),

final_results AS (
SELECT
team_id,
SUM(win) as wins,
SUM(loss) as loss
FROM overall_results
GROUP BY team_id
)

SELECT
t.division_id as division_id,
f.team_id as team_id,
t.team_name as team_name,
f.wins as wins,
f.loss as loss
FROM final_results as f LEFT JOIN teams as t
ON f.team_id = t.id
WHERE t.season = 2025
ORDER BY division_id, wins DESC
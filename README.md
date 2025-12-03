# MLB ETL

First of all, the copyright of the data obtained through this repository is *Copyright 2025 MLB Advanced Media, L.P.* Use of the data obtained through this repository acknowledges agreement to 
the terms posted here [http://gdx.mlb.com/components/copyright.txt].

This repo is for:

- My personal interests in applying statistics to baseball analytics (Sabermetrics).

- Practicing my data extract-transform-load (ETL) skills tailored for my own interests in Sabermetrics.

The ultimate goal of this project, ETL-wise, is to update the game results and player performance (both batter and pitcher) from MLB Stats API everyday during the regular season.

Among many TODOs, the following have higher priority:

- Fix the codes counting stolen_base

- Create the flow for pitcher performance
  (Currently, only batter performance is available.)

- Set up configuration file or Airflow plugins to avoid editing DAG files
  (Unfortunately, we have to hard-coding a little bit, very tiny bit, at the moment.)

## Usage
### Docker

Since this repo is designed to run within the [docker](https://github.com/danicychao/MLB_etl/blob/main/docker-compose.yml), one might want to install things like [Docker Desktop](https://www.docker.com/products/docker-desktop/) and
[pgAdmin](https://www.pgadmin.org/), before starting anything.

For running the ETL pipelines or accessing the tables of the extracted MLB datas, one need to activate the docker first:

`docker compose up -d`

### PostgreSQL
- Log in to pgAdmin (http://localhost:5050) with the ***email and password under pgadmin*** in `docker-compose.yml`

- At the first time in pgAdmin, set up the server following ***postgres_data*** in `docker-compose.yml`

### Airflow
- Log in to Airflow (http://localhost:8080) with the ***username and password under airflow: entrypoint:*** in `docker-compose.yml`

### Flow DAG
- `mlb_etl_team_pipeline` should run annually before the season starts, as the team information might change every season but rarely change during the season. (Temporarily, one need to go inside `mlb_etl_team_dag.py` to change the year of season.)

- Both `mlb_etl_results_pipeline` and `mlb_etl_gamedetails_pipeline` should run daily during the season to update the game results and player performance.

- `mlb_etl_results_pipeline` should run before `mlb_etl_gamedetails_pipeline`. (Temporarily, one need to go inside `daily_mlb_results_etl_dag.py` and `daily_mlb_game_details_etl_dag.py` to change the dates of games.)


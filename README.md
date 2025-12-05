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
- Before the very first time you run the pipelines on the Airflow UI. Add `season_year` and `game_date` in **Variables** under **Admin**.
  One can set 2025 and 2025-03-18, respectively, as the default values.

- `mlb_etl_team_pipeline` should run annually before the season starts, as the team information might change every season but rarely change during the season.
  Change the year of season on the Airflow UI: **Admin** -> **Variables** -> `season_year`.

- Both `mlb_etl_results_pipeline` and `mlb_etl_gamedetails_pipeline` should run daily during the season to update the game results and player performance.

- `mlb_etl_results_pipeline` should run before `mlb_etl_gamedetails_pipeline`.
  Change game date on the Airflow UI: **Admin** -> ***Variables* -> `game_date`.


# MLB Red Sox Analytics Pipeline

A cloud data engineering project built on Google Cloud Platform (GCP) 
analyzing 150+ years of Boston Red Sox and MLB data.

## Architecture
CSV Data → Python ETL → BigQuery (mlb_raw) → dbt Models → BigQuery (mlb_transformed) → Looker Studio Dashboard

## Tools & Technologies
- Python (Pandas, google-cloud-bigquery)
- Google Cloud Platform / BigQuery
- dbt Core with BigQuery adapter
- Looker Studio
- SQL
- GitHub

## Data Source
Lahman Baseball Database (1871-2025)
Records loaded: 240,000+ across 5 tables
- Batting: 128,598 records
- Pitching: 57,630 records
- Teams: 3,614 records
- People: 24,270 records
- Salaries: 26,428 records

## dbt Models
- player_season_stats: Player performance by season with batting average and OBP
- team_performance: All MLB teams historical performance and win percentage
- redsox_history: Boston Red Sox year by year analysis (1871-2025)

## Dashboard
Built in Looker Studio with 2 pages:
1. Red Sox Historical Performance (win percentage + wins/losses by season)
2. Data Quality Monitoring (record counts and completeness tracking)

## Key Findings
- 2018 Red Sox peak season: 108 wins, 0.667 win percentage, World Series champions
- 155 seasons of Red Sox history analyzed
- 2020 shortened season clearly visible: only 24 wins in 60 game season
- Data quality monitoring tracks 3,614 team records across all MLB franchises

## How to Run
1. Clone this repository
2. Set up a GCP project and enable BigQuery
3. Run `python3 pipeline.py` to load data into BigQuery
4. Run `dbt run` inside the mlb_pipeline folder to build models
5. Connect Looker Studio to mlb_transformed dataset
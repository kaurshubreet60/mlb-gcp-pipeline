with teams as (
    select * from `redsox-analytics-pipeline.mlb_raw.teams`
),

batting as (
    select * from `redsox-analytics-pipeline.mlb_raw.batting`
),

people as (
    select * from `redsox-analytics-pipeline.mlb_raw.people`
)

select
    t.yearid as season,
    t.name as team_name,
    t.g as games_played,
    t.w as wins,
    t.l as losses,
    round(safe_divide(t.w, t.g), 3) as win_percentage,
    t.r as runs_scored,
    t.ra as runs_allowed,
    t.r - t.ra as run_differential,
    t.wswin as world_series_winner
from teams t
where t.teamid = 'BOS'
order by t.yearid desc
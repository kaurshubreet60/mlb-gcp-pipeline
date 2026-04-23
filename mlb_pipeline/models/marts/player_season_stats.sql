with batting as (
    select * from `redsox-analytics-pipeline.mlb_raw.batting`
),

people as (
    select * from `redsox-analytics-pipeline.mlb_raw.people`
)

select
    b.playerid,
    p.namefirst || ' ' || p.namelast as player_name,
    b.yearid as season,
    b.teamid as team,
    b.g as games,
    b.ab as at_bats,
    b.h as hits,
    b.hr as home_runs,
    b.rbi,
    b.bb as walks,
    b.so as strikeouts,
    round(safe_divide(b.h, b.ab), 3) as batting_average,
    round(safe_divide(b.h + b.bb, b.ab + b.bb), 3) as on_base_percentage
from batting b
left join people p on b.playerid = p.playerid
where b.ab > 0
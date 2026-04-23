with teams as (
    select * from `redsox-analytics-pipeline.mlb_raw.teams`
)

select
    yearid as season,
    teamid as team,
    name as team_name,
    g as games_played,
    w as wins,
    l as losses,
    round(safe_divide(w, g), 3) as win_percentage,
    r as runs_scored,
    ra as runs_allowed,
    r - ra as run_differential,
    rank() over (partition by yearid order by w desc) as league_rank
from teams
where g > 0
order by yearid desc, wins desc
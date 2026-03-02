{{ config(materialized='table') }}

select
    show_publisher,
    count(distinct show_name)                                           as total_shows,
    count(distinct episode_uri)                                         as total_episodes_charted,
    count(distinct region_code)                                         as regions_present,
    count(distinct snapshot_date)                                       as total_days_on_chart,
    count(*)                                                            as total_rows,
    min(ranking_position)                                               as best_rank_ever,
    round(sum(ranking_position) * 1.0 / count(*), 1)                   as avg_rank,
    count(distinct case when media_type = 'audio'
          then show_name end)                                           as audio_shows,
    count(distinct case when media_type = 'mixed'
          then show_name end)                                           as mixed_shows,
    round(avg(population), 0)                                           as avg_market_population,
    min(snapshot_date)                                                  as first_charted,
    max(snapshot_date)                                                  as last_charted

from {{ ref('int_show_performance') }}
group by
    show_publisher

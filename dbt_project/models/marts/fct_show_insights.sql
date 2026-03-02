{{ config(materialized='table') }}

select
    show_name,
    show_publisher,
    media_type,
    max(show_total_episodes)                    as show_total_episodes,
    count(distinct region_code)                 as regions_present,
    count(distinct episode_uri)                 as unique_episodes_charted,
    count(distinct snapshot_date)               as total_days_on_chart,
    count(*)                                    as total_rows,
    min(ranking_position)                       as best_rank_ever,
    round(sum(ranking_position) * 1.0
          / count(*), 1)                        as avg_rank,
    min(snapshot_date)                          as first_charted,
    max(snapshot_date)                          as last_charted,
    round(avg(population), 0)                   as avg_market_population,
    max(population)                             as largest_market_population,
    min(population)                             as smallest_market_population

from {{ ref('int_show_performance') }}
group by
    show_name,
    show_publisher,
    media_type

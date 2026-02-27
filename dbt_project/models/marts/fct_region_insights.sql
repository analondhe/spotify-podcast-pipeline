{{ config(materialized='table') }}

select
    region_code,
    snapshot_date,
    country_name,
    continent,
    world_region,
    population,

    -- scale
    count(distinct show_name)                                           as total_shows,
    count(distinct episode_uri)                                         as total_episodes,
    count(*)                                                            as total_rows,

    -- content mix raw counts
    count(case when media_type = 'mixed' then 1 end)                   as mixed_media_count,
    count(case when media_type = 'audio' then 1 end)                   as audio_count,
    count(case when explicit then 1 end)                               as explicit_count,
    count(case when chart_rank_move = 'NEW' then 1 end)                as new_entries_count,

    -- business logic percentages
    round(100.0 * count(case when media_type = 'audio' then 1 end)
          / count(*), 1)                                                as pct_audio,
    round(100.0 * count(case when media_type = 'mixed' then 1 end)
          / count(*), 1)                                                as pct_mixed_media,
    round(100.0 * count(case when explicit then 1 end)
          / count(*), 1)                                                as pct_explicit,

    -- listening profile
    round(avg(duration_minutes), 1)                                     as avg_duration_minutes,
    mode(languages)                                                     as dominant_language

from {{ ref('int_region_overview') }}
group by
    region_code,
    snapshot_date,
    country_name,
    continent,
    world_region,
    population

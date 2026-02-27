{{ config(materialized='view') }}

select
    p.region_code,
    p.snapshot_date,
    p.episode_uri,
    p.show_uri,
    p.show_name,
    p.show_publisher,
    p.media_type,
    p.explicit,
    p.duration_minutes,
    p.languages,
    p.chart_rank_move,
    p.ranking_position,
    c.country_name,
    c.continent,
    c.world_region,
    s.population

from {{ ref('stg_podcast_episodes') }} p
left join {{ ref('country_codes') }} c
    on upper(p.region_code) = c.country_code
left join {{ ref('country_stats') }} s
    on upper(p.region_code) = s.country_code

{{ config(materialized='view') }}

select
    p.show_uri,
    p.show_name,
    p.show_publisher,
    p.media_type,
    p.episode_uri,
    p.region_code,
    p.snapshot_date,
    p.ranking_position,
    p.chart_rank_move,
    p.explicit,
    p.duration_minutes,
    p.languages,
    p.release_date,
    p.show_total_episodes,
    s.population

from {{ ref('stg_podcast_episodes') }} p
left join {{ ref('country_stats') }} s
    on upper(p.region_code) = s.country_code

{{ config(materialized='view') }}

with source as (
    select
        date                    as snapshot_date,
        rank                    as ranking_position,
        region                  as region_code,
        "chartRankMove"         as chart_rank_move,
        "episodeUri"            as episode_uri,
        "showUri"               as show_uri,
        "episodeName"           as episode_name,
        description             as episode_description,
        "show.name"             as show_name,
        "show.description"      as show_description,
        "show.publisher"        as show_publisher,
        duration_ms,
        explicit,
        languages,
        release_date,
        release_date_precision,
        "show.media_type"       as media_type,
        "show.total_episodes"   as show_total_episodes
    from {{ source('raw', 'top_podcasts') }}
),

final as (
    select
        cast(snapshot_date as date) as snapshot_date,
        cast(ranking_position as integer) as ranking_position,
        upper(region_code) as region_code,
        chart_rank_move,
        episode_uri,
        show_uri,
        episode_name,
        episode_description,
        show_name,
        show_description,
        show_publisher,
        duration_ms,
        round(duration_ms / 60000.0, 2) as duration_minutes,
        explicit,
        languages,
        cast(release_date as date) as release_date,
        release_date_precision,
        media_type,
        cast(show_total_episodes as integer) as show_total_episodes
    from source
)

select * from final

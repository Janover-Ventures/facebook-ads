{% macro stitch_fb_ad_creatives() %}

    {{ adapter_macro('facebook_ads.stitch_fb_ad_creatives') }}

{% endmacro %}


{% macro default__stitch_fb_ad_creatives() %}

with base as (

    select
        id as creative_id,
        lower(nullif(url_tags, '')) as url_tags,
        lower(coalesce(
        nullif(object_story_spec__link_data__call_to_action__value__link, ''),
        nullif(object_story_spec__video_data__call_to_action__value__link, ''),
        nullif(object_story_spec__link_data__link, '')
        )) as url
    
    from
        {{ var('ad_creatives_table') }}

), 

splits as (

    select
        id,
        url,
        split_part(url ,'?', 1) as base_url,
        --this is a strange thing to have to do but it's because sometimes the URL exists on the story object and we wouldn't get the appropriate UTM params here otherwise
        coalesce(url_tags, split_part(url ,'?', 2)) as url_tags

    from base

)

final as (

    select
        *,
        '/' || split_part(split_part(split_part(url, '//', 2), '/', 2), '?', 1) as url_path,
        split_part(split_part(url, '//', 2), '/', 1) as url_host,
        split_part(split_part(url_tags,'utm_source=',2), '&', 1) as utm_source,
        split_part(split_part(url_tags,'utm_medium=',2), '&', 1) as utm_medium,
        split_part(split_part(url_tags,'utm_campaign=',2), '&', 1) as utm_campaign,
        split_part(split_part(url_tags,'utm_content=',2), '&', 1) as utm_content,
        split_part(split_part(url_tags,'utm_term=',2), '&', 1) as utm_term
    
    from splits

)

select * from final

{% endmacro %}


{% macro snowflake__stitch_fb_ad_creatives() %}

with base as (

    select

        id as creative_id,
        lower(coalesce(
            nullif(object_story_spec['link_data']['call_to_action']['value']['link']::varchar, ''),
            nullif(object_story_spec['video_data']['call_to_action']['value']['link']::varchar, ''),
            nullif(object_story_spec['link_data']['link']::varchar, '')
        )) as url

    from {{ var('ad_creatives_table') }}

),

parsed as (

    select
        id,
        url,
        split_part(url, '?', 1) as base_url,
        parse_url(url)['host']::varchar as url_host,
        '/' || parse_url(url)['path']::varchar as url_path,
        nullif(parse_url(url)['parameters']['utm_campaign']::varchar, '') as utm_campaign,
        nullif(parse_url(url)['parameters']['utm_source']::varchar, '') as utm_source,
        nullif(parse_url(url)['parameters']['utm_medium']::varchar, '') as utm_medium,
        nullif(parse_url(url)['parameters']['utm_content']::varchar, '') as utm_content,
        nullif(parse_url(url)['parameters']['utm_term']::varchar, '') as utm_term

    from base 

)

select * from parsed

{% endmacro %}
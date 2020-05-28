WITH fb_keyword_performance AS (
    SELECT
        *
    FROM
        {{ ref('fb_ad_insights_xf') }}
),
fb_keyword_performance_agg AS (
    SELECT
        CAST(
            date_day AS DATE
        ) AS campaign_date,
        adset_id AS ad_group_id,
        adset_name AS ad_group_name,
        campaign_id,
        url_host,
        url_path,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        campaign_name,
        'facebook ads' AS platform,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        SUM(spend) AS spend
    FROM
        fb_keyword_performance {{ dbt_utils.group_by(13) }}
)
SELECT
    *
FROM
    fb_keyword_performance_agg

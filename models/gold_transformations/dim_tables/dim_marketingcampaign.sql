{{
    config(
        materialized = 'incremental',
        unique_key = 'campaign_key',
        constraints = {
            'primary_key': ['campaign_key']
        }
    )
}}

SELECT
distinct
    MD5(CAST(campaign_id AS STRING)) AS campaign_key,
    campaign_id,
    target_audience,
    budget,
    CAMPAIGN_DURATION_DAYS as duration,
    roi_calculation as roi,
    start_date,
    end_date
FROM {{ source('DBT_SAIFSHAHUL_NEW_DATA', 'STG_MARKETING') }}



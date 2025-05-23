{{
    config(
        materialized="incremental"
    )
}}

with
    cleaned_data as (
        select distinct
            campaign_id,

            {{ standardize_string_column("campaign_name") }} as Campaign_Name,
            to_date(start_date) as Start_Date,
            TO_DATE(end_date) as End_Date,
            replace(replace(budget, '$', ''), ',', '')::float as budget,
            replace(replace(total_cost, '$', ''), ',', '')::float as total_cost,
            replace(replace(total_revenue, '$', ''), ',', '')::float as total_revenue,
            {{ standardize_string_column("target_audience") }} as Target_Audience,
            {{ standardize_string_column("campaign_type") }} as Campaign_Type,
            {{ standardize_string_column("channel") }} as Channel,
            (replace(replace(total_revenue, '$', ''), ',', '')::float - replace(replace(total_cost, '$', ''), ',', '')::float) / replace(replace(total_cost, '$', ''), ',', '')::float as roi_calculation,
            description,
            to_date(last_modified_date) as last_modified_date,
            datediff(
                'day',
                to_date(start_date, 'YYYY-MM-DDTHH:MI:SS'),
                to_date(end_date, 'YYYY-MM-DDTHH:MI:SS')
            ) as campaign_duration_days,
            case
                when target_audience like '%18-25%'
                then 'Young Adults'
                when target_audience like '%26-35%'
                then 'Adults'
                when target_audience like '%36-50%'
                then 'Middle-aged Adults'
                when target_audience like '%51+%'
                then 'Seniors'
                else 'General'
            end as audience_segment
        from {{ source("new_data", "campaign_details_silver") }}
    )

select *
from cleaned_data

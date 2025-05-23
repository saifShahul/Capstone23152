{% snapshot customer_snapshot %}
    {{
        config(
            target_schema='snapshots',
            unique_key='customer_id',
            strategy='timestamp',
            updated_at='last_modified_date'
        )
    }}
 
    select * from {{ source('DBT_SAIFSHAHUL_NEW_DATA', 'STG_CUSTOMERS') }}
 {% endsnapshot %}
 
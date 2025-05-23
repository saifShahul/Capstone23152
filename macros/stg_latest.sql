{% macro get_latest_records(source_table, unique_key, last_modified_column) %}
with ranked_records as (
    select
        *,
        row_number() over (
            partition by {{ unique_key }}
            order by {{ last_modified_column }} desc
        ) as row_num
    from {{ source_table }}
)
 
select
    {% for col in adapter.get_columns_in_relation(source_table) %}
        {% if col.name != 'row_num' %}
            {{ col.name }}{% if not loop.last %}, {% endif %}
        {% endif %}
    {% endfor %}
from ranked_records
where row_num = 1
{% if is_incremental() %}
and {{ last_modified_column }} > (select max({{ last_modified_column }}) from {{ this }})
{% endif %}
{% endmacro %}
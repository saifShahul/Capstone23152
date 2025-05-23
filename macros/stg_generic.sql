{% macro standardize_string_column(column) %}
    upper(trim(regexp_replace({{ column }}, '[^a-zA-Z0-9]', '')))
{% endmacro %}

{% macro standardize_pascal_case(column) %}
    INITCAP({{ column }})
{% endmacro %}


{% macro standardize_date_format(column, default_date='9999-12-31') %}
    case
        when {{ column }} is null then '{{ default_date }}'
        when try_to_timestamp_ntz({{ column }}, 'DD-MM-YYYY') is not null then try_to_timestamp_ntz({{ column }}, 'DD-MM-YYYY')
        else try_to_timestamp_ntz({{ column }}, 'YYYY-MM-DD')
    end
{% endmacro %}


{% macro convert_to_date(column) %}
    coalesce(
        case
            when try_to_timestamp({{ column }}, 'DD-MM-YYYY') is not null then TO_DATE(try_to_timestamp({{ column }}, 'DD-MM-YYYY'))
            when try_to_timestamp({{ column }}, 'YYYY-MM-DD') is not null then TO_DATE(try_to_timestamp({{ column }}, 'YYYY-MM-DD'))
            else TO_DATE('9999-12-31')
        end,
        TO_DATE('9999-12-31')
    )
{% endmacro %}








{% macro validate_data_types(column, expected_type) %}
    case
        when {{ column }} is null then null
        else {{ column }}
    end
{% endmacro %}


{% macro check_null(column) %}
    case
        when {{ column }} is null then 'Missing'
        else {{ column }}
    end
{% endmacro %}

{% macro trim_whitespaces(column) %}
    trim({{ column }})
{% endmacro %}

{% macro standardize_capitalization(column) %}
    upper({{ column }})
{% endmacro %}

{% macro remove_special_characters(column) %}
    regexp_replace({{ column }}, '[^a-zA-Z0-9]', '')
{% endmacro %}

{% macro normalize_phone_number(column) %}
    regexp_replace({{ column }}, '[^0-9]', '')
{% endmacro %}

{% macro validate_email_format(column) %}
    case
        when {{ column }} not like '%_@__%.__%' then null
        else {{ column }}
    end
{% endmacro %}

{% macro convert_currency(column) %}
    cast(regexp_replace({{ column }}, '[^0-9.]', '') as decimal(10,2))
{% endmacro %}

{% macro pascal_case(column) %}
    -- Converts text to Pascal Case (capitalizes each word and removes spaces)
   initcap(trim(regexp_replace({{ column }}, '[^a-zA-Z0-9 ]', '')))
{% endmacro %}

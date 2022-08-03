{{ config(materialized='table') }}

{% set table_list = ( 'eligibility', 'medical_claim' ) -%}

{%- for table_item in table_list %}

    {%- set current_table = var( table_item ) -%}

    {%- set all_columns = adapter.get_columns_in_relation(
        current_table
    ) -%}

    {%- for column_item in all_columns %}

    select
          '{{ table_item }}' as table_name
        , '{{ column_item.name|lower }}' as column_name
        , '{{ column_item.data_type }}' as data_type
        , (
            select count(*)::integer as cnt
            from {{ current_table }}
          ) as table_total
        , (
            select max(len( {{ column_item.name }} ))::integer as cnt
            from {{ current_table }}
          ) as max_column_length
        , (
            select count(*)::integer as cnt
            from {{ current_table }}
            where {{ column_item.name }} is null
          ) as missing_values
        , case
            when table_total =  0 then null
            else ((missing_values::decimal(18,2) / table_total::decimal(18,2))*100)::decimal(18,2)
          end as missing_percentage
        , (
            select count(*)::integer as cnt
            from {{ current_table }}
            where {{ column_item.name }}::varchar = ''
          ) as blank_values
        , case
            when table_total =  0 then null
            else ((blank_values::decimal(18,2) / table_total::decimal(18,2))*100)::decimal(18,2)
          end as blank_percentage
        , (
            select count(distinct {{ column_item.name }} )::integer as ct
            from {{ current_table }}
          ) as unique_values
        , case
            when table_total =  0 then null
            else ((unique_values::decimal(18,2) / table_total::decimal(18,2))*100)::decimal(18, 2)
          end as unique_percentage

    {% if not loop.last -%}
        union all
    {%- endif -%}

    {%- endfor -%}

    {% if not loop.last -%}
        union all
    {%- endif -%}

{%- endfor -%}
{{ config(materialized='view') }}

{#-
    setting vars used in for loops
-#}
{% set table_list = [
      'eligibility'
    , 'medical_claim'
] -%}

{% set date_col_list = [
      'birth_date'
    , 'death_date'
    , 'claim_start_date'
    , 'claim_end_date'
    , 'admission_date'
    , 'discharge_date'
    , 'claim_line_start_date'
    , 'claim_line_end_date'
    , 'paid_date'
    , 'procedure_date_1'
    , 'procedure_date_2'
    , 'procedure_date_3'
    , 'procedure_date_4'
    , 'procedure_date_5'
    , 'procedure_date_6'
    , 'procedure_date_7'
    , 'procedure_date_8'
    , 'procedure_date_9'
    , 'procedure_date_10'
    , 'procedure_date_11'
    , 'procedure_date_12'
    , 'procedure_date_13'
    , 'procedure_date_14'
    , 'procedure_date_15'
    , 'procedure_date_16'
    , 'procedure_date_17'
    , 'procedure_date_18'
    , 'procedure_date_19'
    , 'procedure_date_20'
    , 'procedure_date_21'
    , 'procedure_date_22'
    , 'procedure_date_23'
    , 'procedure_date_24'
    , 'procedure_date_25'
] -%}

/*
    loop through specific date columns for min/max checks
*/

with date_checks as (

    {%- for table_item in table_list %}

        {%- set current_table = var( table_item ) -%}

        {%- set all_columns = adapter.get_columns_in_relation(
            current_table
        ) -%}

        {%- for column_item in all_columns
            if column_item.name.lower() in date_col_list %}

            select
                  '{{ table_item }}' as table_name
                , '{{ column_item.name|lower }}' as column_name
                , (select min( {{ column_item.name }} ) as min_date
                   from {{ current_table }}
                  ) as min_date
                , (select max( {{ column_item.name }} ) as max_date
                   from {{ current_table }}
                  ) as max_date

            {% if not loop.last -%}
                union all
            {%- endif -%}

            {%- endfor -%}

        {% if not loop.last -%}
            union all
        {%- endif -%}

    {%- endfor -%}

)

select
      table_name
    , column_name
    , min_date
    , max_date
from date_checks
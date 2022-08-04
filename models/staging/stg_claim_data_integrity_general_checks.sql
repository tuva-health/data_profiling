{{ config(materialized='ephemeral') }}

{#-
    setting vars used in for loops
-#}
{% set table_list = [
      'eligibility'
    , 'medical_claim'
] -%}

/*
    loop through all tables and columns for general checks
*/
with general_checks as (

    {{ data_integrity_general_checks(table_list) }}

)

select
      table_name
    , column_name
    , data_type
    , table_total
    , max_column_length
    , missing_values
    , blank_values
    , unique_values
from general_checks
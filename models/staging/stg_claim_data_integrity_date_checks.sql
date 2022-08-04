{{ config(materialized='ephemeral') }}

{#-
    setting vars used in for loops
-#}
{% set table_list = [
      'eligibility'
    , 'medical_claim'
] -%}

{% set column_list = [
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

    {{ data_integrity_date_checks(table_list, column_list) }}

)

select
      table_name
    , column_name
    , min_date
    , max_date
from date_checks
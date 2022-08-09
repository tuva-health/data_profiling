{{ config(materialized='table') }}

{#
    setting var to determine unique patients from member months
#}
{% set unique_patient_id_count -%}
(select count(*)
from (
    select distinct patient_id, birth_date
    from {{ var('eligibility') }}
))
{% endset -%}

{#
    setting var to determine unique claims from claim line
#}
{% set unique_claim_id_count -%}
(select count(*)
from {{ var('medical_claim') }}
where claim_line_number = 1)
{% endset -%}

with general_checks as (

    select *
    from {{ ref('stg_claim_data_integrity_general_checks') }}

),

date_checks as (

    select *
    from {{ ref('stg_claim_data_integrity_date_checks') }}

),

date_relevance_checks as (

    select *
    from {{ ref('stg_claim_data_integrity_date_relevance_checks') }}

),

referential_integrity_checks as (

    select *
    from {{ ref('stg_claim_data_integrity_referential_checks') }}

),

valid_value_checks as (

    select *
    from {{ ref('stg_claim_data_integrity_valid_values') }}

),

/*
    join all checks together to create summary output and calculate percentages
*/
joined as (
    select
          general_checks.table_name
        , general_checks.column_name
        , general_checks.data_type
        , general_checks.table_total
        , general_checks.max_column_length
        , general_checks.missing_values
        , case
            when general_checks.table_total =  0 then null
            else ((general_checks.missing_values::decimal(18,2) /
                   general_checks.table_total::decimal(18,2))*100
                 )::decimal(18,2)
          end as missing_percentage
        , general_checks.blank_values
        , case
            when general_checks.table_total =  0 then null
            else ((general_checks.blank_values::decimal(18,2) /
                   general_checks.table_total::decimal(18,2))*100
                 )::decimal(18,2)
          end as blank_percentage
        , general_checks.unique_values
        , case
            when general_checks.table_total =  0 then null
            {# unique total logic for specific fields -#}
            when (general_checks.table_name = 'eligibility'
              and general_checks.column_name = 'patient_id')
              then ((general_checks.unique_values::decimal(18,2) /
                     {{ unique_patient_id_count }}::decimal(18,2))*100
                   )::decimal(18, 2)
            when (general_checks.table_name = 'medical_claim'
              and general_checks.column_name = 'claim_id')
              then ((general_checks.unique_values::decimal(18,2) /
                     {{ unique_claim_id_count }}::decimal(18,2))*100
                   )::decimal(18, 2)
            else ((general_checks.unique_values::decimal(18,2) /
                   general_checks.table_total::decimal(18,2))*100
                 )::decimal(18, 2)
          end as unique_percentage
        , date_checks.min_date
        , date_checks.max_date
        , date_relevance_checks.date_relevance_check
        , date_relevance_checks.date_relevance_errors
        , case
            when general_checks.table_total =  0 then null
            else ((date_relevance_checks.date_relevance_errors::decimal(18,2) /
                   general_checks.table_total::decimal(18,2))*100
                 )::decimal(18, 2)
          end as date_relevance_errors_percentage
        , referential_integrity_checks.referential_integrity_check
        , referential_integrity_checks.referential_integrity_errors
        , case
            when general_checks.table_total =  0 then null
            else ((referential_integrity_checks.referential_integrity_errors::decimal(18,2) /
                   general_checks.table_total::decimal(18,2))*100
                 )::decimal(18, 2)
          end as referential_integrity_errors_percentage
        , valid_value_checks.valid_value_check
        , valid_value_checks.valid_value_errors
        , case
            when general_checks.table_total =  0 then null
            else ((valid_value_checks.valid_value_errors::decimal(18,2) /
                   general_checks.table_total::decimal(18,2))*100
                 )::decimal(18, 2)
          end as valid_value_errors_percentage
    from general_checks
         left join date_checks
           on general_checks.table_name = date_checks.table_name
           and general_checks.column_name = date_checks.column_name
         left join date_relevance_checks
           on  general_checks.table_name = date_relevance_checks.table_name
           and general_checks.column_name = date_relevance_checks.column_name
         left join referential_integrity_checks
           on  general_checks.table_name = referential_integrity_checks.table_name
           and general_checks.column_name = referential_integrity_checks.column_name
         left join valid_value_checks
           on  general_checks.table_name = valid_value_checks.table_name
           and general_checks.column_name = valid_value_checks.column_name

)

select
      table_name
    , column_name
    , data_type
    , table_total
    , max_column_length
    , missing_values
    , missing_percentage
    , blank_values
    , blank_percentage
    , unique_values
    , unique_percentage
    , min_date
    , max_date
    , date_relevance_check
    , date_relevance_errors
    , date_relevance_errors_percentage
    , referential_integrity_check
    , referential_integrity_errors
    , referential_integrity_errors_percentage
    , valid_value_check
    , valid_value_errors
    , valid_value_errors_percentage
    , getdate()::datetime as run_date
from joined
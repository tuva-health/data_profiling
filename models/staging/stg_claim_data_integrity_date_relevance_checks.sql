{{ config(materialized='ephemeral') }}

with death_date_check as (

    select
          'eligibility' as table_name
        , 'death_date' as column_name
        , 'death date before birth date' as date_relevance_check
        , count(*) as date_relevance_errors
    from {{ var('eligibility') }}
    where death_date < birth_date

),

discharge_date_check as (

    select
          'medical_claim' as table_name
        , 'discharge_date' as column_name
        , 'discharge date before admission date' as date_relevance_check
        , count(*) as date_relevance_errors
    from {{ var('medical_claim') }}
    where discharge_date < admission_date

),

claim_end_date_check as (

    select
          'medical_claim' as table_name
        , 'claim_end_date' as column_name
        , 'claim end date before start date' as date_relevance_check
        , count(*) as date_relevance_errors
    from {{ var('medical_claim') }}
    where claim_end_date < claim_start_date

),

claim_line_end_date as (

    select
          'medical_claim' as table_name
        , 'claim_line_end_date' as column_name
        , 'claim line end date before start date' as date_relevance_check
        , count(*) as date_relevance_errors
    from {{ var('medical_claim') }}
    where claim_line_end_date < claim_line_start_date

),

union_checks as (

    select * from death_date_check
    union all
    select * from discharge_date_check
    union all
    select * from claim_end_date_check
    union all
    select * from claim_line_end_date

)

select
      table_name
    , column_name
    , date_relevance_check
    , date_relevance_errors
from union_checks
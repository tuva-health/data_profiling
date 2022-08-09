{{ config(materialized='view') }}

with seed_bill_type as (

    select * from {{ ref('bill_type_code') }}

),

seed_claim_type as (

    select * from {{ ref('claim_type') }}

),

seed_discharge_disposition as (

    select * from {{ ref('discharge_disposition') }}

),

seed_place_of_service as (

    select * from {{ ref('place_of_service') }}

),

seed_revenue_center as (

    select * from {{ ref('revenue_center_code') }}

),

bill_type_check as (

    select
          'medical_claim' as table_name
        , 'bill_type_code' as column_name
        , 'valid value - bill type codes' as valid_value_check
        , count(*) as valid_value_errors
    from {{ var('medical_claim') }} as medical_claim
         left join seed_bill_type
         on medical_claim.bill_type_code::varchar =
            seed_bill_type.code::varchar
    where medical_claim.bill_type_code is not null
    and seed_bill_type.code is null

),

claim_type_check as (

    select
          'medical_claim' as table_name
        , 'claim_type' as column_name
        , 'valid value - claim type' as valid_value_check
        , count(*) as valid_value_errors
    from {{ var('medical_claim') }} as medical_claim
         left join seed_claim_type
         on medical_claim.claim_type = seed_claim_type.code
    where medical_claim.claim_type is not null
    and seed_claim_type.code is null

),

discharge_disposition_check as (

    select
          'medical_claim' as table_name
        , 'discharge_disposition_code' as column_name
        , 'valid value - discharge disposition code' as valid_value_check
        , count(*) as valid_value_errors
    from {{ var('medical_claim') }} as medical_claim
         left join seed_discharge_disposition
         on medical_claim.discharge_disposition_code::varchar =
            seed_discharge_disposition.discharge_disposition_code::varchar
    where medical_claim.discharge_disposition_code is not null
    and seed_discharge_disposition.discharge_disposition_code is null

),

place_of_service_check as (

    select
          'medical_claim' as table_name
        , 'place_of_service_code' as column_name
        , 'valid value - place of service code' as valid_value_check
        , count(*) as valid_value_errors
    from {{ var('medical_claim') }} as medical_claim
         left join seed_place_of_service
         on medical_claim.place_of_service_code::varchar =
            seed_place_of_service.place_of_service_code::varchar
    where medical_claim.place_of_service_code is not null
    and seed_place_of_service.place_of_service_code is null

),

revenue_center_check as (

    select
          'medical_claim' as table_name
        , 'revenue_center_code' as column_name
        , 'valid value - revenue center code' as valid_value_check
        , count(*) as valid_value_errors
    from {{ var('medical_claim') }} as medical_claim
         left join seed_revenue_center
         on medical_claim.revenue_center_code::varchar =
            seed_revenue_center.revenue_center_code::varchar
    where medical_claim.revenue_center_code is not null
    and seed_revenue_center.revenue_center_code is null

),

union_checks as (

    select * from bill_type_check
    union all
    select * from claim_type_check
    union all
    select * from discharge_disposition_check
    union all
    select * from place_of_service_check
    union all
    select * from revenue_center_check

)

select
      table_name
    , column_name
    , valid_value_check
    , valid_value_errors
from union_checks
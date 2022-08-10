{{ config(materialized='view') }}

with medical_claim_patient_id_check as (

    select
          'medical_claim' as table_name
        , 'patient_id' as column_name
        , 'foreign key patient_id missing in patient' as referential_integrity_check
        , count(*) as referential_integrity_errors
    from {{ var('medical_claim') }} as medical_claim
         left join {{ var('eligibility') }} as eligibility
         on medical_claim.patient_id = eligibility.patient_id
    where eligibility.patient_id is null

),

union_checks as (

    select * from medical_claim_patient_id_check

)

select
      table_name
    , column_name
    , referential_integrity_check
    , referential_integrity_errors
from union_checks
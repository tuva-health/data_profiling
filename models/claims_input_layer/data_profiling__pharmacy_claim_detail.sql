{{ config(
    enabled=var('data_profiling_enabled',var('tuva_packages_enabled',True))
) }}

with pharmacy_claim as (

    select * from {{ ref('data_profiling__base_pharmacy_claim') }}

),

duplicate_record as (

    select row_hash
    from pharmacy_claim
    group by row_hash
    having count (*) > 1

),

duplicate_claim_id as (

    select distinct claim_id
    from (
        select
              claim_id
            , claim_line_number
        from pharmacy_claim
        group by
              claim_id
            , claim_line_number
        having count (*) > 1
    )

),

missing_fk_patient_id as (

    select distinct pharmacy_claim.row_hash
    from pharmacy_claim
         left join {{ ref('data_profiling__base_eligibility') }} as eligibility
         on pharmacy_claim.patient_id = eligibility.patient_id
    where eligibility.patient_id is null

),

joined as (

    select
          pharmacy_claim.claim_id
        , pharmacy_claim.claim_line_number
        , case
            when duplicate_record.row_hash is null then 0
            else 1
          end as duplicate_pharm_claim_record
        , case
            when duplicate_claim_id.claim_id is null then 0
            else 1
          end as duplicate_pharm_claim_id
        , {{ missing_field_check('pharmacy_claim.patient_id') }} as missing_pharm_claim_patient_id
        , case
            when missing_fk_patient_id.row_hash is null then 0
            else 1
          end as missing_pharm_claim_patient_id_fk
        , {{ missing_field_check('pharmacy_claim.claim_id') }} as missing_pharm_claim_id
        , {{ missing_field_check('pharmacy_claim.claim_line_number') }} as missing_pharm_claim_line_number
        , {{ missing_field_check('pharmacy_claim.dispensing_date') }} as missing_dispensing_date
        , {{ valid_claim_date_check('pharmacy_claim.dispensing_date') }} as invalid_dispensing_date
        , {{ missing_field_check('pharmacy_claim.paid_date') }} as missing_pharm_claim_paid_date
        , {{ valid_claim_date_check('pharmacy_claim.paid_date') }} as invalid_pharm_claim_paid_date
        , {{ missing_field_check('pharmacy_claim.paid_amount') }} as missing_pharm_claim_paid_amount
        , {{ missing_field_check('pharmacy_claim.prescribing_provider_npi') }} as missing_prescribing_provider_npi
        , {{ missing_field_check('pharmacy_claim.dispensing_provider_npi') }} as missing_dispensing_provider_npi
        , {{ missing_field_check('pharmacy_claim.ndc_code') }} as missing_ndc_code
    from pharmacy_claim
         left join duplicate_record
            on pharmacy_claim.row_hash = duplicate_record.row_hash
         left join duplicate_claim_id
            on pharmacy_claim.claim_id = duplicate_claim_id.claim_id
         left join missing_fk_patient_id
            on pharmacy_claim.row_hash = missing_fk_patient_id.row_hash

)

select
      {{ cast_string_or_varchar('claim_id') }} as claim_id
    , {{ cast_string_or_varchar('claim_line_number') }} as claim_line_number
    , duplicate_pharm_claim_record
    , duplicate_pharm_claim_id
    , missing_pharm_claim_patient_id
    , missing_pharm_claim_patient_id_fk
    , missing_pharm_claim_id
    , missing_pharm_claim_line_number
    , missing_dispensing_date
    , invalid_dispensing_date
    , missing_pharm_claim_paid_date
    , invalid_pharm_claim_paid_date
    , missing_pharm_claim_paid_amount
    , missing_prescribing_provider_npi
    , missing_dispensing_provider_npi
    , missing_ndc_code
    , {{ current_date_or_timestamp('timestamp') }} as run_date
from joined
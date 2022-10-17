with pharmacy_claim as (

    select * from {{ ref('base_pharmacy_claim') }}

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

    select row_hash
    from pharmacy_claim
         left join {{ var('eligibility') }} as eligibility
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
          end as duplicate_record_pharm
        , case
            when duplicate_claim_id.claim_id is null then 0
            else 1
          end as duplicate_claim_id_pharm
        , case
            when missing_fk_patient_id.row_hash is null then 0
            else 1
          end as missing_fk_patient_id_pharm
        , {{ missing_field_check('pharmacy_claim.claim_id') }} as missing_claim_id_pharm
        , {{ missing_field_check('pharmacy_claim.claim_line_number') }} as missing_claim_line_number_pharm
        , {{ missing_field_check('pharmacy_claim.patient_id') }} as missing_patient_id_pharm
        , {{ missing_field_check('pharmacy_claim.prescribing_provider_npi') }} as missing_prescribing_provider_npi_pharm
        , {{ missing_field_check('pharmacy_claim.dispensing_provider_npi') }} as missing_dispensing_provider_npi_pharm
        , {{ missing_field_check('pharmacy_claim.dispensing_date') }} as missing_dispensing_date_pharm
        , {{ missing_field_check('pharmacy_claim.ndc') }} as missing_ndc_pharm
        , {{ missing_field_check('pharmacy_claim.paid_amount') }} as missing_paid_amount_pharm
        , {{ missing_field_check('pharmacy_claim.paid_date') }} as missing_paid_date_pharm
        , {{ valid_past_or_current_date_check('pharmacy_claim.dispensing_date') }} as invalid_dispensing_date_pharm
        , {{ valid_past_or_current_date_check('pharmacy_claim.paid_date') }} as invalid_paid_date_pharm
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
    , duplicate_record_pharm
    , duplicate_claim_id_pharm
    , missing_fk_patient_id_pharm
    , missing_claim_id_pharm
    , missing_claim_line_number_pharm
    , missing_patient_id_pharm
    , missing_prescribing_provider_npi_pharm
    , missing_dispensing_provider_npi_pharm
    , missing_dispensing_date_pharm
    , missing_ndc_pharm
    , missing_paid_amount_pharm
    , missing_paid_date_pharm
    , invalid_dispensing_date_pharm
    , invalid_paid_date_pharm
    , {{ current_date_or_timestamp('timestamp') }} as run_date
from joined
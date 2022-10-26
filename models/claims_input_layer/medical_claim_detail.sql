with medical_claim as (

    select * from {{ ref('base_medical_claim') }}

),

seed_bill_type as (

    select * from {{ ref('bill_type') }}

),

seed_claim_type as (

    select * from {{ ref('claim_type') }}

),

seed_discharge_disposition as (

    select * from {{ ref('discharge_disposition') }}

),

seed_icd_10_cm as (

    select * from {{ ref('icd_10_cm') }}

),

seed_ms_drg as (

    select * from {{ ref('ms_drg') }}

),

seed_place_of_service as (

    select * from {{ ref('place_of_service') }}

),

seed_present_on_admission as (

    select * from {{ ref('present_on_admission') }}

),

seed_revenue_center as (

    select * from {{ ref('revenue_center_code') }}

),

duplicate_record as (

    select row_hash
    from medical_claim
    group by row_hash
    having count (*) > 1

),

duplicate_claim_id as (

    select distinct claim_id
    from (
        select
              claim_id
            , claim_line_number
        from medical_claim
        group by
              claim_id
            , claim_line_number
        having count (*) > 1
    )

),

missing_fk_patient_id as (

    select distinct medical_claim.row_hash
    from medical_claim
         left join {{ ref('base_eligibility') }} as eligibility
         on medical_claim.patient_id = eligibility.patient_id
    where eligibility.patient_id is null

),

joined as (

    select
          medical_claim.claim_id
        , medical_claim.claim_line_number
        , case
            when duplicate_record.row_hash is null then 0
            else 1
          end as duplicate_med_claim_record
        , case
            when duplicate_claim_id.claim_id is null then 0
            else 1
          end as duplicate_med_claim_id
        , {{ missing_field_check('medical_claim.patient_id') }} as missing_med_claim_patient_id
        , case
            when missing_fk_patient_id.row_hash is null then 0
            else 1
          end as missing_med_claim_patient_id_fk
        , {{ missing_field_check('medical_claim.claim_id') }} as missing_med_claim_id
        , {{ missing_field_check('medical_claim.claim_line_number') }} as missing_med_claim_line_number
        , {{ missing_field_check('medical_claim.claim_type') }} as missing_claim_type
        , case
            when medical_claim.claim_type is null then 0
            when seed_claim_type.description is not null then 0
            else 1
          end as invalid_claim_type
        , {{ missing_field_check('medical_claim.claim_start_date') }} as missing_claim_start_date
        , {{ valid_claim_date_check('medical_claim.claim_start_date') }} as invalid_claim_start_date
        , {{ missing_field_check('medical_claim.claim_end_date') }} as missing_claim_end_date
        , {{ valid_claim_date_check('medical_claim.claim_end_date') }} as invalid_claim_end_date
        , case
            when medical_claim.claim_end_date is null then 0
            when medical_claim.claim_end_date is not null
              and medical_claim.claim_end_date >= medical_claim.claim_start_date
              then 0
            else 1
          end as invalid_claim_end_before_start
        , case
            when medical_claim.claim_type = 'institutional'
            then {{ missing_field_check('medical_claim.admission_date') }}
            else 0
          end as missing_admission_date
        , case
            when medical_claim.claim_type = 'institutional'
            then {{ valid_claim_date_check('medical_claim.admission_date') }}
            else 0
          end as invalid_admission_date
        , case
            when medical_claim.claim_type = 'institutional'
            then {{ missing_field_check('medical_claim.discharge_date') }}
            else 0
          end as missing_discharge_date
        , case
            when medical_claim.claim_type = 'institutional'
            then {{ valid_claim_date_check('medical_claim.discharge_date') }}
            else 0
          end as invalid_discharge_date
        , case
            when medical_claim.discharge_date is null then 0
            when medical_claim.discharge_date is not null
              and medical_claim.discharge_date >= medical_claim.admission_date
              then 0
            else 1
          end as invalid_discharge_before_admission
        , {{ missing_field_check('medical_claim.paid_date') }} as missing_med_claim_paid_date
        , {{ valid_claim_date_check('medical_claim.paid_date') }} as invalid_med_claim_paid_date
        , {{ missing_field_check('medical_claim.paid_amount') }} as missing_med_claim_paid_amount
        , case
            when medical_claim.claim_type = 'institutional'
            then {{ missing_field_check('medical_claim.bill_type_code') }}
            else 0
          end as missing_bill_type_code
        , case
            when medical_claim.bill_type_code is null then 0
            when seed_bill_type.code is not null then 0
            else 1
          end as invalid_bill_type_code
        , case
            when medical_claim.claim_type = 'professional'
            then {{ missing_field_check('medical_claim.place_of_service_code') }}
            else 0
          end as missing_place_of_service_code
        , case
            when medical_claim.place_of_service_code is null then 0
            when seed_place_of_service.place_of_service_code is not null then 0
            else 1
          end invalid_place_of_service_code
        , case
            when medical_claim.claim_type = 'institutional'
            then {{ missing_field_check('medical_claim.revenue_center_code') }}
            else 0
          end as missing_revenue_center_code
        , case
            when medical_claim.revenue_center_code is null then 0
            when seed_revenue_center.revenue_center_code is not null then 0
            else 1
          end invalid_revenue_center_code
        , {{ missing_field_check('medical_claim.diagnosis_code_1') }} as missing_diagnosis_code_1
        , case
            when medical_claim.diagnosis_code_1 is null then 0
            when seed_icd_10_cm.icd_10_cm is not null then 0
            else 1
          end invalid_diagnosis_code_1
       , case
            when medical_claim.claim_type = 'institutional'
            then {{ missing_field_check('medical_claim.diagnosis_poa_1') }}
            else 0
          end as missing_diagnosis_poa_1
       , case
            when medical_claim.diagnosis_poa_1 is null then 0
            when seed_present_on_admission.present_on_admit_code is not null then 0
            else 1
          end invalid_diagnosis_poa_1
        , case
            when medical_claim.claim_type = 'professional'
            then {{ missing_field_check('medical_claim.hcpcs_code') }}
            else 0
          end as missing_hcpcs_code
        , case
            when medical_claim.discharge_disposition_code is null then 0
            when seed_discharge_disposition.discharge_disposition_code is not null then 0
            else 1
          end invalid_discharge_disposition_code
        , case
            when medical_claim.ms_drg is null then 0
            when seed_ms_drg.code is not null then 0
            else 1
          end invalid_ms_drg
        , case
            when medical_claim.claim_type = 'professional'
            then {{ missing_field_check('medical_claim.billing_npi') }}
            else 0
          end as missing_billing_npi
        , case
            when medical_claim.claim_type = 'institutional'
            then {{ missing_field_check('medical_claim.facility_npi') }}
            else 0
          end as missing_facility_npi
        , {{ missing_field_check('medical_claim.rendering_npi') }} as missing_rendering_npi
    from medical_claim
         left join duplicate_record
            on medical_claim.row_hash = duplicate_record.row_hash
         left join duplicate_claim_id
            on medical_claim.claim_id = duplicate_claim_id.claim_id
         left join missing_fk_patient_id
            on medical_claim.row_hash = missing_fk_patient_id.row_hash
         left join seed_bill_type
            on medical_claim.bill_type_code = seed_bill_type.code
         left join seed_claim_type
            on medical_claim.claim_type = seed_claim_type.description
         left join seed_discharge_disposition
            on medical_claim.discharge_disposition_code = seed_discharge_disposition.discharge_disposition_code
         left join seed_icd_10_cm
            on medical_claim.diagnosis_code_1 = seed_icd_10_cm.icd_10_cm
         left join seed_ms_drg
            on medical_claim.ms_drg = seed_ms_drg.code
         left join seed_place_of_service
            on medical_claim.place_of_service_code = seed_place_of_service.place_of_service_code
         left join seed_present_on_admission
            on medical_claim.diagnosis_poa_1 = seed_present_on_admission.present_on_admit_code
         left join seed_revenue_center
            on medical_claim.revenue_center_code = seed_revenue_center.revenue_center_code

)

/* casting fields used as unique key in snapshot */
select
      {{ cast_string_or_varchar('claim_id') }} as claim_id
    , {{ cast_string_or_varchar('claim_line_number') }} as claim_line_number
    , duplicate_med_claim_record
    , duplicate_med_claim_id
    , missing_med_claim_patient_id
    , missing_med_claim_patient_id_fk
    , missing_med_claim_id
    , missing_med_claim_line_number
    , missing_claim_type
    , invalid_claim_type
    , missing_claim_start_date
    , invalid_claim_start_date
    , missing_claim_end_date
    , invalid_claim_end_date
    , invalid_claim_end_before_start
    , missing_admission_date
    , invalid_admission_date
    , missing_discharge_date
    , invalid_discharge_date
    , invalid_discharge_before_admission
    , missing_med_claim_paid_date
    , invalid_med_claim_paid_date
    , missing_med_claim_paid_amount
    , missing_bill_type_code
    , invalid_bill_type_code
    , missing_place_of_service_code
    , invalid_place_of_service_code
    , missing_revenue_center_code
    , invalid_revenue_center_code
    , missing_diagnosis_code_1
    , invalid_diagnosis_code_1
    , missing_diagnosis_poa_1
    , invalid_diagnosis_poa_1
    , missing_hcpcs_code
    , invalid_discharge_disposition_code
    , invalid_ms_drg
    , missing_billing_npi
    , missing_facility_npi
    , missing_rendering_npi
    , {{ current_date_or_timestamp('timestamp') }} as run_date
from joined
with medical_claim_src as (

    select * from {{ var('medical_claim') }}

),

seed_bill_type as (

    select * from {{ ref('bill_type_code') }}

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

    select
          claim_id
        , claim_line_number
    from (
        select
              claim_id, claim_line_number, patient_id, claim_start_date
            , claim_end_date, claim_line_start_date, claim_line_end_date
            , admission_date, discharge_date, claim_type, bill_type_code
            , place_of_service_code, admit_source_code, admit_type_code
            , discharge_disposition_code, ms_drg, revenue_center_code
            , service_unit_quantity, hcpcs_code, hcpcs_modifier_1
            , hcpcs_modifier_2, hcpcs_modifier_3, hcpcs_modifier_4
            , hcpcs_modifier_5, billing_npi, rendering_npi, facility_npi
            , paid_date, paid_amount, charge_amount, adjustment_type_code
            , diagnosis_code_1, diagnosis_code_2, diagnosis_code_3
            , diagnosis_code_4, diagnosis_code_5, diagnosis_code_6
            , diagnosis_code_7, diagnosis_code_8, diagnosis_code_9
            , diagnosis_code_10, diagnosis_code_11, diagnosis_code_12
            , diagnosis_code_13, diagnosis_code_14, diagnosis_code_15
            , diagnosis_code_16, diagnosis_code_17, diagnosis_code_18
            , diagnosis_code_19, diagnosis_code_20, diagnosis_code_21
            , diagnosis_code_22, diagnosis_code_23, diagnosis_code_24
            , diagnosis_code_25, diagnosis_poa_1, diagnosis_poa_2
            , diagnosis_poa_3, diagnosis_poa_4, diagnosis_poa_5
            , diagnosis_poa_6, diagnosis_poa_7, diagnosis_poa_8
            , diagnosis_poa_9, diagnosis_poa_10, diagnosis_poa_11
            , diagnosis_poa_12, diagnosis_poa_13, diagnosis_poa_14
            , diagnosis_poa_15, diagnosis_poa_16, diagnosis_poa_17
            , diagnosis_poa_18, diagnosis_poa_19, diagnosis_poa_20
            , diagnosis_poa_21, diagnosis_poa_22, diagnosis_poa_23
            , diagnosis_poa_24, diagnosis_poa_25, diagnosis_code_type
            , procedure_code_type, procedure_code_1, procedure_code_2
            , procedure_code_3, procedure_code_4, procedure_code_5
            , procedure_code_6, procedure_code_7, procedure_code_8
            , procedure_code_9, procedure_code_10, procedure_code_11
            , procedure_code_12, procedure_code_13, procedure_code_14
            , procedure_code_15, procedure_code_16, procedure_code_17
            , procedure_code_18, procedure_code_19, procedure_code_20
            , procedure_code_21, procedure_code_22, procedure_code_23
            , procedure_code_24, procedure_code_25, procedure_date_1
            , procedure_date_2, procedure_date_3, procedure_date_4
            , procedure_date_5, procedure_date_6, procedure_date_7
            , procedure_date_8, procedure_date_9, procedure_date_10
            , procedure_date_11, procedure_date_12, procedure_date_13
            , procedure_date_14, procedure_date_15, procedure_date_16
            , procedure_date_17, procedure_date_18, procedure_date_19
            , procedure_date_20, procedure_date_21, procedure_date_22
            , procedure_date_23, procedure_date_24, procedure_date_25
        from medical_claim_src
        group by
              claim_id, claim_line_number, patient_id, claim_start_date
            , claim_end_date, claim_line_start_date, claim_line_end_date
            , admission_date, discharge_date, claim_type, bill_type_code
            , place_of_service_code, admit_source_code, admit_type_code
            , discharge_disposition_code, ms_drg, revenue_center_code
            , service_unit_quantity, hcpcs_code, hcpcs_modifier_1
            , hcpcs_modifier_2, hcpcs_modifier_3, hcpcs_modifier_4
            , hcpcs_modifier_5, billing_npi, rendering_npi, facility_npi
            , paid_date, paid_amount, charge_amount, adjustment_type_code
            , diagnosis_code_1, diagnosis_code_2, diagnosis_code_3
            , diagnosis_code_4, diagnosis_code_5, diagnosis_code_6
            , diagnosis_code_7, diagnosis_code_8, diagnosis_code_9
            , diagnosis_code_10, diagnosis_code_11, diagnosis_code_12
            , diagnosis_code_13, diagnosis_code_14, diagnosis_code_15
            , diagnosis_code_16, diagnosis_code_17, diagnosis_code_18
            , diagnosis_code_19, diagnosis_code_20, diagnosis_code_21
            , diagnosis_code_22, diagnosis_code_23, diagnosis_code_24
            , diagnosis_code_25, diagnosis_poa_1, diagnosis_poa_2
            , diagnosis_poa_3, diagnosis_poa_4, diagnosis_poa_5
            , diagnosis_poa_6, diagnosis_poa_7, diagnosis_poa_8
            , diagnosis_poa_9, diagnosis_poa_10, diagnosis_poa_11
            , diagnosis_poa_12, diagnosis_poa_13, diagnosis_poa_14
            , diagnosis_poa_15, diagnosis_poa_16, diagnosis_poa_17
            , diagnosis_poa_18, diagnosis_poa_19, diagnosis_poa_20
            , diagnosis_poa_21, diagnosis_poa_22, diagnosis_poa_23
            , diagnosis_poa_24, diagnosis_poa_25, diagnosis_code_type
            , procedure_code_type, procedure_code_1, procedure_code_2
            , procedure_code_3, procedure_code_4, procedure_code_5
            , procedure_code_6, procedure_code_7, procedure_code_8
            , procedure_code_9, procedure_code_10, procedure_code_11
            , procedure_code_12, procedure_code_13, procedure_code_14
            , procedure_code_15, procedure_code_16, procedure_code_17
            , procedure_code_18, procedure_code_19, procedure_code_20
            , procedure_code_21, procedure_code_22, procedure_code_23
            , procedure_code_24, procedure_code_25, procedure_date_1
            , procedure_date_2, procedure_date_3, procedure_date_4
            , procedure_date_5, procedure_date_6, procedure_date_7
            , procedure_date_8, procedure_date_9, procedure_date_10
            , procedure_date_11, procedure_date_12, procedure_date_13
            , procedure_date_14, procedure_date_15, procedure_date_16
            , procedure_date_17, procedure_date_18, procedure_date_19
            , procedure_date_20, procedure_date_21, procedure_date_22
            , procedure_date_23, procedure_date_24, procedure_date_25
        having count (*) > 1
    )

),

duplicate_claim_id as (

    select claim_id
    from medical_claim_src
    where claim_line_number = 1
    group by claim_id
    having count(*) > 1

),

missing_fk_patient_id as (

    select distinct claim_id
    from medical_claim_src
         left join {{ var('eligibility') }} as eligibility
         on medical_claim_src.patient_id = eligibility.patient_id
    where eligibility.patient_id is null

),

joined as (

    select
          medical_claim_src.claim_id
        , medical_claim_src.claim_line_number
        , case
            when duplicate_record.claim_id is null then 0
            else 1
          end as duplicate_record_flag
        , case
            when duplicate_claim_id.claim_id is null then 0
            else 1
          end as duplicate_claim_id_flag
        , case
            when missing_fk_patient_id.claim_id is null then 0
            else 1
          end as missing_fk_patient_id_flag
        , {{ missing_field_check('medical_claim_src.claim_id') }} as missing_claim_id_flag
        , {{ missing_field_check('medical_claim_src.claim_line_number') }} as missing_claim_line_number_flag
        , {{ missing_field_check('medical_claim_src.patient_id') }} as missing_patient_id_flag
        , {{ missing_field_check('medical_claim_src.claim_start_date') }} as missing_claim_start_date_flag
        , {{ missing_field_check('medical_claim_src.claim_end_date') }} as missing_claim_end_date_flag
        , {{ missing_field_check('medical_claim_src.admission_date') }} as missing_admission_date_flag
        , {{ missing_field_check('medical_claim_src.discharge_date') }} as missing_discharge_date_flag
        , {{ missing_field_check('medical_claim_src.claim_type') }} as missing_claim_type_flag
        , case
            when medical_claim_src.claim_type = 'I'
            then {{ missing_field_check('medical_claim_src.bill_type_code') }}
            else 0
          end as missing_bill_type_code_flag
        , case
            when medical_claim_src.claim_type = 'P'
            then {{ missing_field_check('medical_claim_src.place_of_service_code') }}
            else 0
          end as missing_place_of_service_code_flag
        , case
            when medical_claim_src.claim_type = 'I'
            then {{ missing_field_check('medical_claim_src.discharge_disposition_code') }}
            else 0
          end as missing_discharge_disposition_code_flag
        , case
            when medical_claim_src.claim_type = 'I'
            then {{ missing_field_check('medical_claim_src.ms_drg') }}
            else 0
          end as missing_ms_drg_flag
        , case
            when medical_claim_src.claim_type = 'I'
            then {{ missing_field_check('medical_claim_src.revenue_center_code') }}
            else 0
          end as missing_revenue_center_code_flag
        , case
            when medical_claim_src.claim_type = 'I'
            then {{ missing_field_check('medical_claim_src.hcpcs_code') }}
            else 0
          end as missing_hcpcs_code_flag
        , {{ missing_field_check('medical_claim_src.billing_npi') }} as missing_billing_npi_flag
        , {{ missing_field_check('medical_claim_src.rendering_npi') }} as missing_rendering_npi_flag
        , {{ missing_field_check('medical_claim_src.facility_npi') }} as missing_facility_npi_flag
        , {{ missing_field_check('medical_claim_src.paid_date') }} as missing_paid_date_flag
        , {{ missing_field_check('medical_claim_src.paid_amount') }} as missing_paid_amount_flag
        , {{ missing_field_check('medical_claim_src.diagnosis_code_1') }} as missing_diagnosis_code_1_flag
        , case
            when medical_claim_src.claim_type = 'I'
            then {{ missing_field_check('medical_claim_src.diagnosis_poa_1') }}
            else 0
          end as missing_diagnosis_poa_1_flag
        , {{ valid_past_or_current_date_check('medical_claim_src.claim_start_date') }} as invalid_claim_start_date_flag
        , {{ valid_past_or_current_date_check('medical_claim_src.claim_end_date') }} as invalid_claim_end_date_flag
        , {{ valid_past_or_current_date_check('medical_claim_src.admission_date') }} as invalid_admission_date_flag
        , {{ valid_past_or_current_date_check('medical_claim_src.discharge_date') }} as invalid_discharge_date_flag
        , {{ valid_past_or_current_date_check('medical_claim_src.paid_date') }} as invalid_paid_date_flag
        , case
            when medical_claim_src.claim_end_date is null then 0
            when medical_claim_src.claim_end_date is not null
              and medical_claim_src.claim_end_date >= medical_claim_src.claim_start_date
              then 0
            else 1
          end as invalid_claim_end_before_start_flag
        , case
            when medical_claim_src.discharge_date is null then 0
            when medical_claim_src.discharge_date is not null
              and medical_claim_src.discharge_date >= medical_claim_src.admission_date
              then 0
            else 1
          end as invalid_discharge_before_admission_flag
        , case
            when medical_claim_src.claim_type is null then 0
            when seed_claim_type.code is not null then 0
            else 1
          end as invalid_claim_type_flag
        , case
            when medical_claim_src.bill_type_code is null then 0
            when seed_bill_type.code is not null then 0
            else 1
          end as invalid_bill_type_code_flag
        , case
            when medical_claim_src.place_of_service_code is null then 0
            when seed_place_of_service.place_of_service_code is not null then 0
            else 1
          end invalid_place_of_service_code_flag
        , case
            when medical_claim_src.discharge_disposition_code is null then 0
            when seed_discharge_disposition.discharge_disposition_code is not null then 0
            else 1
          end invalid_discharge_disposition_code_flag
        , case
            when medical_claim_src.ms_drg is null then 0
            when seed_ms_drg.ms_drg is not null then 0
            else 1
          end invalid_ms_drg_flag
        , case
            when medical_claim_src.revenue_center_code is null then 0
            when seed_revenue_center.revenue_center_code is not null then 0
            else 1
          end invalid_revenue_center_code_flag
        , case
            when medical_claim_src.diagnosis_code_1 is null then 0
            when seed_icd_10_cm.icd_10_cm is not null then 0
            else 1
          end invalid_diagnosis_code_1_flag
        , case
            when medical_claim_src.diagnosis_poa_1 is null then 0
            when seed_present_on_admission.present_on_admit_code is not null then 0
            else 1
          end invalid_diagnosis_poa_1_flag
    from medical_claim_src
         left join duplicate_record
            on medical_claim_src.claim_id = duplicate_record.claim_id
            and medical_claim_src.claim_line_number = duplicate_record.claim_line_number
         left join duplicate_claim_id
            on medical_claim_src.claim_id = duplicate_claim_id.claim_id
         left join missing_fk_patient_id
            on medical_claim_src.claim_id = missing_fk_patient_id.claim_id
         left join seed_bill_type
            on medical_claim_src.bill_type_code = seed_bill_type.code
         left join seed_claim_type
            on medical_claim_src.claim_type = seed_claim_type.code
         left join seed_discharge_disposition
            on medical_claim_src.discharge_disposition_code = seed_discharge_disposition.discharge_disposition_code
         left join seed_icd_10_cm
            on medical_claim_src.diagnosis_code_1 = seed_icd_10_cm.icd_10_cm
         left join seed_ms_drg
            on medical_claim_src.ms_drg = seed_ms_drg.ms_drg
         left join seed_place_of_service
            on medical_claim_src.place_of_service_code = seed_place_of_service.place_of_service_code
         left join seed_present_on_admission
            on medical_claim_src.diagnosis_poa_1 = seed_present_on_admission.present_on_admit_code
         left join seed_revenue_center
            on medical_claim_src.revenue_center_code = seed_revenue_center.revenue_center_code

)

select
      claim_id
    , claim_line_number
    , duplicate_record_flag
    , duplicate_claim_id_flag
    , missing_fk_patient_id_flag
    , missing_claim_id_flag
    , missing_claim_line_number_flag
    , missing_patient_id_flag
    , missing_claim_start_date_flag
    , missing_claim_end_date_flag
    , missing_admission_date_flag
    , missing_discharge_date_flag
    , missing_claim_type_flag
    , missing_bill_type_code_flag
    , missing_place_of_service_code_flag
    , missing_discharge_disposition_code_flag
    , missing_ms_drg_flag
    , missing_revenue_center_code_flag
    , missing_hcpcs_code_flag
    , missing_billing_npi_flag
    , missing_rendering_npi_flag
    , missing_facility_npi_flag
    , missing_paid_date_flag
    , missing_paid_amount_flag
    , missing_diagnosis_code_1_flag
    , missing_diagnosis_poa_1_flag
    , invalid_claim_start_date_flag
    , invalid_claim_end_date_flag
    , invalid_admission_date_flag
    , invalid_discharge_date_flag
    , invalid_paid_date_flag
    , invalid_claim_end_before_start_flag
    , invalid_discharge_before_admission_flag
    , invalid_claim_type_flag
    , invalid_bill_type_code_flag
    , invalid_place_of_service_code_flag
    , invalid_discharge_disposition_code_flag
    , invalid_ms_drg_flag
    , invalid_revenue_center_code_flag
    , invalid_diagnosis_code_1_flag
    , invalid_diagnosis_poa_1_flag
    , getdate()::datetime as run_date
from joined
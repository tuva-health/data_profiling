{#-
    ***************************************************************
    setting vars for unique counts, total counts, and test columns
    ***************************************************************
-#}

{% set institutional_claim_count -%}
    (select count(*)
    from {{ var('medical_claim') }}
    where claim_type = 'I')
{% endset -%}

{% set professional_claim_count -%}
    (select count(*)
    from {{ var('medical_claim') }}
    where claim_type = 'P')
{% endset -%}

{% set total_eligibility_count -%}
    (select count(*)
     from {{ var('eligibility') }})
{% endset -%}

{% set total_claim_count -%}
    (select count(*)
    from {{ var('medical_claim') }})
{% endset -%}

{% set unique_patient_id_count -%}
    (select count(*)
    from (
        select distinct patient_id, payer
        from {{ var('eligibility') }}
    ))
{% endset -%}

{% set unique_claim_id_count -%}
    (select count(*)
    from {{ var('medical_claim') }}
    where claim_line_number = 1)
{% endset -%}

{% set eligibility_column_list = [
      'duplicate_record_flag'
    , 'duplicate_patient_id_flag'
    , 'missing_patient_id_flag'
    , 'missing_month_flag'
    , 'missing_year_flag'
    , 'missing_gender_flag'
    , 'missing_birth_date_flag'
    , 'missing_death_date_flag'
    , 'invalid_birth_date_flag'
    , 'invalid_death_date_flag'
    , 'invalid_death_before_birth_flag'
    , 'invalid_gender_flag'
] -%}

{% set medical_claim_column_list = [
      'duplicate_record_flag'
    , 'duplicate_claim_id_flag'
    , 'missing_fk_patient_id_flag'
    , 'missing_claim_id_flag'
    , 'missing_claim_line_number_flag'
    , 'missing_patient_id_flag'
    , 'missing_claim_start_date_flag'
    , 'missing_claim_end_date_flag'
    , 'missing_admission_date_flag'
    , 'missing_discharge_date_flag'
    , 'missing_claim_type_flag'
    , 'missing_bill_type_code_flag'
    , 'missing_place_of_service_code_flag'
    , 'missing_discharge_disposition_code_flag'
    , 'missing_ms_drg_flag'
    , 'missing_revenue_center_code_flag'
    , 'missing_hcpcs_code_flag'
    , 'missing_billing_npi_flag'
    , 'missing_rendering_npi_flag'
    , 'missing_facility_npi_flag'
    , 'missing_paid_date_flag'
    , 'missing_paid_amount_flag'
    , 'missing_diagnosis_code_1_flag'
    , 'missing_diagnosis_poa_1_flag'
    , 'invalid_claim_start_date_flag'
    , 'invalid_claim_end_date_flag'
    , 'invalid_admission_date_flag'
    , 'invalid_discharge_date_flag'
    , 'invalid_paid_date_flag'
    , 'invalid_claim_end_before_start_flag'
    , 'invalid_discharge_before_admission_flag'
    , 'invalid_claim_type_flag'
    , 'invalid_bill_type_code_flag'
    , 'invalid_place_of_service_code_flag'
    , 'invalid_discharge_disposition_code_flag'
    , 'invalid_ms_drg_flag'
    , 'invalid_revenue_center_code_flag'
    , 'invalid_diagnosis_code_1_flag'
    , 'invalid_diagnosis_poa_1_flag'
] -%}

with eligibility_detail as (

    select * from {{ ref('eligibility_detail') }}

),

medical_claim_detail as (

    select * from {{ ref('medical_claim_detail') }}

),

sum_eligibility_detail as (

    {{ sum_all_checks_in_table('eligibility_detail', eligibility_column_list) }}

),

sum_medical_claim_detail as (

    {{ sum_all_checks_in_table('medical_claim_detail', medical_claim_column_list) }}

),

add_denominator_eligibility_detail as (

    select
          table_name
        , test_name
        , test_fail_numerator
        , case
            when test_name = 'duplicate_patient_id_flag' then {{ unique_patient_id_count }}
            else {{ total_eligibility_count }}
          end as test_fail_denominator
    from sum_eligibility_detail

),

add_denominator_medical_claim_detail as (

    select
          table_name
        , test_name
        , test_fail_numerator
        , case
            when test_name in (
                  'missing_bill_type_code_flag'
                , 'missing_discharge_disposition_code_flag'
                , 'missing_ms_drg_flag'
                , 'missing_revenue_center_code_flag'
                , 'missing_hcpcs_code_flag'
                , 'missing_diagnosis_poa_1_flag'
               ) then {{ institutional_claim_count }}
            when test_name = 'missing_place_of_service_code_flag' then {{ professional_claim_count }}
            when test_name = 'duplicate_claim_id_flag' then {{ unique_claim_id_count }}
            else {{ total_claim_count }}
          end as test_fail_denominator
    from sum_medical_claim_detail

),

add_totals_eligibility_detail as (

    select
          table_name
        , test_name
        , test_fail_numerator
        , test_fail_denominator
        , (round(test_fail_numerator / test_fail_denominator, 4)
          )*100 as test_fail_percentage
    from add_denominator_eligibility_detail

),

add_totals_medical_claim_detail as (

    select
          table_name
        , test_name
        , test_fail_numerator
        , test_fail_denominator
        , (round(test_fail_numerator / test_fail_denominator, 4)
          )*100 as test_fail_percentage
    from add_denominator_medical_claim_detail

),

union_details as (

    select * from add_totals_eligibility_detail
    union all
    select * from add_totals_medical_claim_detail

)

select
      table_name
    , test_name
    , test_fail_numerator
    , test_fail_denominator
    , test_fail_percentage
    , {{ current_date_or_timestamp('timestamp') }} as run_date
from union_details
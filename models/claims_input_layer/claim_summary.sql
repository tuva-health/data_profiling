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

{% set eligibility_column_list = [
      'duplicate_record_elig'
    , 'duplicate_patient_id_elig'
    , 'missing_patient_id_elig'
    , 'missing_month_elig'
    , 'missing_year_elig'
    , 'missing_gender_elig'
    , 'missing_birth_date_elig'
    , 'missing_death_date_elig'
    , 'invalid_birth_date_elig'
    , 'invalid_death_date_elig'
    , 'invalid_death_before_birth_elig'
    , 'invalid_gender_elig'
] -%}

{% set medical_claim_column_list = [
      'duplicate_record_med'
    , 'duplicate_claim_id_med'
    , 'missing_fk_patient_id_med'
    , 'missing_claim_id_med'
    , 'missing_claim_line_number_med'
    , 'missing_patient_id_med'
    , 'missing_claim_start_date_med'
    , 'missing_claim_end_date_med'
    , 'missing_admission_date_med'
    , 'missing_discharge_date_med'
    , 'missing_claim_type_med'
    , 'missing_bill_type_code_med'
    , 'missing_place_of_service_code_med'
    , 'missing_discharge_disposition_code_med'
    , 'missing_ms_drg_med'
    , 'missing_revenue_center_code_med'
    , 'missing_hcpcs_code_med'
    , 'missing_billing_npi_med'
    , 'missing_rendering_npi_med'
    , 'missing_facility_npi_med'
    , 'missing_paid_date_med'
    , 'missing_paid_amount_med'
    , 'missing_diagnosis_code_1_med'
    , 'missing_diagnosis_poa_1_med'
    , 'invalid_claim_start_date_med'
    , 'invalid_claim_end_date_med'
    , 'invalid_admission_date_med'
    , 'invalid_discharge_date_med'
    , 'invalid_paid_date_med'
    , 'invalid_claim_end_before_start_med'
    , 'invalid_discharge_before_admission_med'
    , 'invalid_claim_type_med'
    , 'invalid_bill_type_code_med'
    , 'invalid_place_of_service_code_med'
    , 'invalid_discharge_disposition_code_med'
    , 'invalid_ms_drg_med'
    , 'invalid_revenue_center_code_med'
    , 'invalid_diagnosis_code_1_med'
    , 'invalid_diagnosis_poa_1_med'
] -%}

with eligibility_detail as (

    select * from {{ ref('eligibility_detail') }}

),

medical_claim_detail as (

    select * from {{ ref('medical_claim_detail') }}

),

seed_test_catalog as (

    select * from {{ ref('test_catalog') }}

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
        , {{ total_eligibility_count }} as test_fail_denominator
    from sum_eligibility_detail

),

add_denominator_medical_claim_detail as (

    select
          table_name
        , test_name
        , test_fail_numerator
        , case
            when test_name in (
                  'missing_bill_type_code_med'
                , 'missing_discharge_disposition_code_med'
                , 'missing_ms_drg_med'
                , 'missing_revenue_center_code_med'
                , 'missing_hcpcs_code_med'
                , 'missing_diagnosis_poa_1_med'
               ) then {{ institutional_claim_count }}
            when test_name = 'missing_place_of_service_code_med' then {{ professional_claim_count }}
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

),

add_catalog_details as (

    select
          union_details.table_name as test_table_name
        , union_details.test_name
        , union_details.test_fail_numerator
        , union_details.test_fail_denominator
        , union_details.test_fail_percentage
        , seed_test_catalog.source_table_name
        , seed_test_catalog.columns
        , seed_test_catalog.test_id
        , case
            when union_details.test_fail_numerator > 0
            then seed_test_catalog.blocking_error_flag
            else 0
          end as blocking_error_flag
    from union_details
         left join seed_test_catalog
         on union_details.test_name = seed_test_catalog.test_name

)

select
      test_id
    , test_name
    , test_table_name
    , source_table_name
    , columns
    , test_fail_numerator
    , test_fail_denominator
    , test_fail_percentage
    , blocking_error_flag
    , {{ current_date_or_timestamp('timestamp') }} as run_date
from add_catalog_details
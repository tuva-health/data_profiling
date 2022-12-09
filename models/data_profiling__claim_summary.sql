
-- depends on: {{ var('eligibility') }}
-- depends on: {{ var('medical_claim') }}
-- depends on: {{ var('pharmacy_claim') }}
{% if target.type in ('redshift') -%}
{{
  config(
    enabled=var('data_profiling_enabled',var('tuva_packages_enabled',True))
    ,sort = ['test_id']
  )
}}
{%- elif target.type in ('bigquery', 'snowflake') -%}
{{
  config(
    enabled=var('data_profiling_enabled',var('tuva_packages_enabled',True))
    ,cluster_by = ['test_id']
  )
}}
{%- else -%}
{%- endif -%}

{#-
    ***************************************************************
    setting vars for unique counts, total counts, and test columns
    ***************************************************************
-#}

{% set institutional_claim_count -%}
    (select count(*)
     from {{ ref('data_profiling__base_medical_claim') }}
     where claim_type = 'institutional')
{% endset -%}

{% set professional_claim_count -%}
    (select count(*)
     from {{ ref('data_profiling__base_medical_claim') }}
     where claim_type = 'professional')
{% endset -%}

{% set total_eligibility_count -%}
    (select count(*)
     from {{ ref('data_profiling__base_eligibility') }})
{% endset -%}

{% set total_med_claim_count -%}
    (select count(*)
     from {{ ref('data_profiling__base_medical_claim') }})
{% endset -%}

{% set total_pharm_claim_count -%}
    (select count(*)
     from {{ ref('data_profiling__base_pharmacy_claim') }})
{% endset -%}

{% set eligibility_column_list = [
      'duplicate_eligibility_record'
    , 'duplicate_patient_id'
    , 'missing_eligibility_patient_id'
    , 'missing_eligibility_member_id'
    , 'missing_enrollment_start_date'
    , 'invalid_enrollment_start_date'
    , 'missing_enrollment_end_date'
    , 'invalid_enrollment_end_date'
    , 'invalid_enrollment_end_before_start'
    , 'missing_birth_date'
    , 'invalid_birth_date'
    , 'missing_death_date'
    , 'invalid_death_date'
    , 'invalid_death_before_birth'
    , 'missing_gender'
    , 'invalid_gender'
] -%}

{% set medical_claim_column_list = [
      'duplicate_med_claim_record'
    , 'duplicate_med_claim_id'
    , 'missing_med_claim_patient_id'
    , 'missing_med_claim_patient_id_fk'
    , 'missing_med_claim_id'
    , 'missing_med_claim_line_number'
    , 'missing_claim_type'
    , 'invalid_claim_type'
    , 'missing_claim_start_date'
    , 'invalid_claim_start_date'
    , 'missing_claim_end_date'
    , 'invalid_claim_end_date'
    , 'invalid_claim_end_before_start'
    , 'missing_admission_date'
    , 'invalid_admission_date'
    , 'missing_discharge_date'
    , 'invalid_discharge_date'
    , 'invalid_discharge_before_admission'
    , 'missing_med_claim_paid_date'
    , 'invalid_med_claim_paid_date'
    , 'missing_med_claim_paid_amount'
    , 'missing_bill_type_code'
    , 'invalid_bill_type_code'
    , 'missing_place_of_service_code'
    , 'invalid_place_of_service_code'
    , 'missing_revenue_center_code'
    , 'invalid_revenue_center_code'
    , 'missing_diagnosis_code_1'
    , 'invalid_diagnosis_code_1'
    , 'missing_diagnosis_poa_1'
    , 'invalid_diagnosis_poa_1'
    , 'missing_hcpcs_code'
    , 'invalid_discharge_disposition_code'
    , 'invalid_ms_drg_code'
    , 'missing_billing_npi'
    , 'missing_facility_npi'
    , 'missing_rendering_npi'
] -%}

{% set pharmacy_claim_column_list = [
      'duplicate_pharm_claim_record'
    , 'duplicate_pharm_claim_id'
    , 'missing_pharm_claim_patient_id'
    , 'missing_pharm_claim_patient_id_fk'
    , 'missing_pharm_claim_id'
    , 'missing_pharm_claim_line_number'
    , 'missing_dispensing_date'
    , 'invalid_dispensing_date'
    , 'missing_pharm_claim_paid_date'
    , 'invalid_pharm_claim_paid_date'
    , 'missing_pharm_claim_paid_amount'
    , 'missing_prescribing_provider_npi'
    , 'missing_dispensing_provider_npi'
    , 'missing_ndc_code'
] -%}

with eligibility_detail as (

    select * from {{ ref('data_profiling__eligibility_detail') }}

),

medical_claim_detail as (

    select * from {{ ref('data_profiling__medical_claim_detail') }}

),

pharmacy_claim_detail as (

    select * from {{ ref('data_profiling__pharmacy_claim_detail') }}

),

seed_test_catalog as (

    select * from {{ ref('data_profiling__test_catalog') }}

),

sum_eligibility_detail as (

    {{ sum_all_checks_in_table(builtins.ref('data_profiling__eligibility_detail'), eligibility_column_list) }}

),

sum_medical_claim_detail as (

    {{ sum_all_checks_in_table(builtins.ref('data_profiling__medical_claim_detail'), medical_claim_column_list) }}

),

sum_pharmacy_claim_detail as (

    {{ sum_all_checks_in_table(builtins.ref('data_profiling__pharmacy_claim_detail'), pharmacy_claim_column_list) }}

),




add_denominator_eligibility_detail as (

    select
          table_name as test_table_name
        ,'{{ var("eligibility") }}' as source_table_name
        , test_name
        , test_fail_numerator
        , {{ total_eligibility_count }} as test_fail_denominator
    from sum_eligibility_detail

),


add_denominator_medical_claim_detail as (

    select
          table_name as test_table_name
        , '{{ var("medical_claim") }}' as source_table_name
        , test_name
        , test_fail_numerator
        , case
            when test_name in (
                  'invalid_admission_date'
                , 'invalid_discharge_date'
                , 'missing_admission_date'
                , 'missing_bill_type_code'
                , 'missing_diagnosis_poa_1'
                , 'missing_discharge_date'
                , 'missing_facility_npi'
                , 'missing_revenue_center_code'
               ) then {{ institutional_claim_count }}
            when test_name in (
                  'missing_billing_npi'
                , 'missing_hcpcs_code'
                , 'missing_place_of_service_code'
                ) then {{ professional_claim_count }}
            else {{ total_med_claim_count }}
          end as test_fail_denominator
    from sum_medical_claim_detail

),

add_denominator_pharmacy_claim_detail as (

    select
          table_name as test_table_name
        , '{{ var("pharmacy_claim") }}' as source_table_name
        , test_name
        , test_fail_numerator
        , {{ total_pharm_claim_count }} as test_fail_denominator
    from sum_pharmacy_claim_detail

),

add_totals_eligibility_detail as (

    select
          test_table_name
        , source_table_name
        , test_name
        , test_fail_numerator
        , test_fail_denominator
        , case when test_fail_denominator = 0 then 0 else  (round(test_fail_numerator / test_fail_denominator, 5)
          )*100  end as test_fail_percentage
    from add_denominator_eligibility_detail

),

add_totals_medical_claim_detail as (

    select
          test_table_name
        , source_table_name
        , test_name
        , test_fail_numerator
        , test_fail_denominator
        , case when test_fail_denominator = 0 then 0 else  (round(test_fail_numerator / test_fail_denominator, 5)
          )*100  end  test_fail_percentage
    from add_denominator_medical_claim_detail

),

add_totals_pharmacy_claim_detail as (

    select
          test_table_name
        , source_table_name
        , test_name
        , test_fail_numerator
        , test_fail_denominator
        , case when test_fail_denominator = 0 then 0 else  (round(test_fail_numerator / test_fail_denominator, 5)
          )*100  end  as test_fail_percentage
    from add_denominator_pharmacy_claim_detail

),

union_details as (

    select * from add_totals_eligibility_detail
    union all
    select * from add_totals_medical_claim_detail
    union all
    select * from add_totals_pharmacy_claim_detail

),

add_catalog_details as (

    select
          union_details.test_table_name
        , union_details.source_table_name
        , union_details.test_name
        , union_details.test_fail_numerator
        , union_details.test_fail_denominator
        , union_details.test_fail_percentage
        , seed_test_catalog.columns
        , seed_test_catalog.test_id
        , seed_test_catalog.description as test_description
        , seed_test_catalog.severity as test_severity
    from union_details
         left join seed_test_catalog
         on union_details.test_name = seed_test_catalog.test_name

)

select
      test_id
    , test_name
    , source_table_name
    , test_severity
    , test_fail_percentage
    , test_fail_numerator
    , test_fail_denominator
    , test_description
    , test_table_name
    , columns
    , {{ current_date_or_timestamp('timestamp') }} as run_date
from add_catalog_details

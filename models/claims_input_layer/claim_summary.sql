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
      'test_0001'
    , 'test_0002'
    , 'test_0003'
    , 'test_0004'
    , 'test_0005'
    , 'test_0006'
    , 'test_0007'
    , 'test_0008'
    , 'test_0009'
    , 'test_0010'
    , 'test_0011'
    , 'test_0012'
] -%}

{% set medical_claim_column_list = [
      'test_0013'
    , 'test_0014'
    , 'test_0015'
    , 'test_0016'
    , 'test_0017'
    , 'test_0018'
    , 'test_0019'
    , 'test_0020'
    , 'test_0021'
    , 'test_0022'
    , 'test_0023'
    , 'test_0024'
    , 'test_0025'
    , 'test_0026'
    , 'test_0027'
    , 'test_0028'
    , 'test_0029'
    , 'test_0030'
    , 'test_0031'
    , 'test_0032'
    , 'test_0033'
    , 'test_0034'
    , 'test_0035'
    , 'test_0036'
    , 'test_0037'
    , 'test_0038'
    , 'test_0039'
    , 'test_0040'
    , 'test_0041'
    , 'test_0042'
    , 'test_0043'
    , 'test_0044'
    , 'test_0045'
    , 'test_0046'
    , 'test_0047'
    , 'test_0048'
    , 'test_0049'
    , 'test_0050'
    , 'test_0051'
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
        , test_id
        , test_fail_numerator
        , case
            when test_id = 'test_0002' /*duplicate_patient_id_flag*/ then {{ unique_patient_id_count }}
            else {{ total_eligibility_count }}
          end as test_fail_denominator
    from sum_eligibility_detail

),

add_denominator_medical_claim_detail as (

    select
          table_name
        , test_id
        , test_fail_numerator
        , case
            when test_id in (
                  'test_0024' /*missing_bill_type_code_flag*/
                , 'test_0026' /*missing_discharge_disposition_code_flag*/
                , 'test_0027' /*missing_ms_drg_flag*/
                , 'test_0028' /*missing_revenue_center_code_flag*/
                , 'test_0029' /*missing_hcpcs_code_flag*/
                , 'test_0036' /*missing_diagnosis_poa_1_flag*/
               ) then {{ institutional_claim_count }}
            when test_id = 'test_0025' /*missing_place_of_service_code_flag*/ then {{ professional_claim_count }}
            when test_id = 'test_0014' /*duplicate_claim_id_flag*/ then {{ unique_claim_id_count }}
            else {{ total_claim_count }}
          end as test_fail_denominator
    from sum_medical_claim_detail

),

add_totals_eligibility_detail as (

    select
          table_name
        , test_id
        , test_fail_numerator
        , test_fail_denominator
        , (round(test_fail_numerator / test_fail_denominator, 4)
          )*100 as test_fail_percentage
    from add_denominator_eligibility_detail

),

add_totals_medical_claim_detail as (

    select
          table_name
        , test_id
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
    , test_id
    , test_fail_numerator
    , test_fail_denominator
    , test_fail_percentage
    , {{ current_date_or_timestamp('timestamp') }} as run_date
from union_details
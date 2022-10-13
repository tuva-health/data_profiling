/*
    Not all data sources may exist. This block of code uses the relation_exists
    macro to check if a source exists. If the source does not exist it is logged
    and an empty pharmacy claim table is used instead.
*/
with pharmacy_claim_src as (

    {% set relation_exists = (load_relation(source(var('source_name'),'pharmacy_claim'))) is not none -%}

    {%- if relation_exists -%}
    {{- log("Pharmacy claim source exists.", info=true) -}}

    select * from {{ var('pharmacy_claim') }}

    {%- else -%}
    {{- log("Pharmacy claim source doesn't exist using blank table instead.", info=true) -}}

    select
          null as claim_id
        , null as claim_line_number
        , null as patient_id
        , null as prescribing_provider_npi
        , null as prescribing_provider_name
        , null as dispensing_provider_npi
        , null as dispensing_provider_name
        , null as dispensing_provider_address
        , null as dispensing_provider_city
        , null as dispensing_provider_state
        , null as dispensing_provider_zip_code
        , null as dispensing_date
        , null as ndc
        , null as quantity
        , null as days_supply
        , null as refills
        , null as paid_date
        , null as paid_amount
        , null as allowed_amount
    where false

    {%- endif %}

),

pharmacy_claim_with_row_key as (

    select *
         , {{ dbt_utils.surrogate_key([
                  'claim_id'
                , 'claim_line_number'
                , 'patient_id'
                , 'prescribing_provider_npi'
                , 'prescribing_provider_name'
                , 'dispensing_provider_npi'
                , 'dispensing_provider_name'
                , 'dispensing_provider_address'
                , 'dispensing_provider_city'
                , 'dispensing_provider_state'
                , 'dispensing_provider_zip_code'
                , 'dispensing_date'
                , 'ndc'
                , 'quantity'
                , 'days_supply'
                , 'allowed_amount'
                , 'paid_amount'
                , 'paid_date'
               ]) }}
           as row_hash
    from pharmacy_claim_src
),

duplicate_record as (

    select row_hash
    from pharmacy_claim_with_row_key
    group by row_hash
    having count (*) > 1

),

duplicate_claim_id as (

    select distinct claim_id
    from (
        select
              claim_id
            , claim_line_number
        from pharmacy_claim_with_row_key
        group by
              claim_id
            , claim_line_number
        having count (*) > 1
    )

),

missing_fk_patient_id as (

    select row_hash
    from pharmacy_claim_with_row_key
         left join {{ var('eligibility') }} as eligibility
         on pharmacy_claim_with_row_key.patient_id = eligibility.patient_id
    where eligibility.patient_id is null

),

joined as (

    select
          pharmacy_claim_with_row_key.claim_id
        , pharmacy_claim_with_row_key.claim_line_number
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
        , {{ missing_field_check('pharmacy_claim_with_row_key.claim_id') }} as missing_claim_id_pharm
        , {{ missing_field_check('pharmacy_claim_with_row_key.claim_line_number') }} as missing_claim_line_number_pharm
        , {{ missing_field_check('pharmacy_claim_with_row_key.patient_id') }} as missing_patient_id_pharm
        , {{ missing_field_check('pharmacy_claim_with_row_key.prescribing_provider_npi') }} as missing_prescribing_provider_npi_pharm
        , {{ missing_field_check('pharmacy_claim_with_row_key.dispensing_provider_npi') }} as missing_dispensing_provider_npi_pharm
        , {{ missing_field_check('pharmacy_claim_with_row_key.dispensing_date') }} as missing_dispensing_date_pharm
        , {{ missing_field_check('pharmacy_claim_with_row_key.ndc') }} as missing_ndc_pharm
        , {{ missing_field_check('pharmacy_claim_with_row_key.paid_amount') }} as missing_paid_amount_pharm
        , {{ missing_field_check('pharmacy_claim_with_row_key.paid_date') }} as missing_paid_date_pharm
        , {{ valid_past_or_current_date_check('pharmacy_claim_with_row_key.dispensing_date') }} as invalid_dispensing_date_pharm
        , {{ valid_past_or_current_date_check('pharmacy_claim_with_row_key.paid_date') }} as invalid_paid_date_pharm
    from pharmacy_claim_with_row_key
         left join duplicate_record
            on pharmacy_claim_with_row_key.row_hash = duplicate_record.row_hash
         left join duplicate_claim_id
            on pharmacy_claim_with_row_key.claim_id = duplicate_claim_id.claim_id
         left join missing_fk_patient_id
            on pharmacy_claim_with_row_key.row_hash = missing_fk_patient_id.row_hash

)

select
      claim_id
    , claim_line_number
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
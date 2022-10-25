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
    {{- log("Pharmacy claim source doesn't exist using an empty table instead.", info=true) -}}

    /*
        casting fields used in joins and tested to correct data types
        integer fields do not need casting
    */
    select
          {{ cast_string_or_varchar('null') }} as claim_id
        , null as claim_line_number
        , {{ cast_string_or_varchar('null') }} as patient_id
        , {{ cast_string_or_varchar('null') }} as member_id
        , null as prescribing_provider_npi
        , null as dispensing_provider_npi
        , cast(null as date) as dispensing_date
        , null as ndc
        , null as quantity
        , null as days_supply
        , null as refills
        , cast(null as date) as paid_date
        , null as paid_amount
        , null as allowed_amount
        , null as data_source
    limit 0

    {%- endif %}

),

pharmacy_claim_with_row_hash as (

    select *
         , {{ dbt_utils.surrogate_key([
                  'claim_id'
                , 'claim_line_number'
                , 'patient_id'
                , 'member_id'
                , 'prescribing_provider_npi'
                , 'dispensing_provider_npi'
                , 'dispensing_date'
                , 'ndc'
                , 'quantity'
                , 'days_supply'
                , 'refills'
                , 'paid_date'
                , 'paid_amount'
                , 'allowed_amount'
                , 'data_source'
               ]) }}
           as row_hash
    from pharmacy_claim_src

)

select * from pharmacy_claim_with_row_hash
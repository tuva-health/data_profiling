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
        casting fields used in joins and tests to correct data types
        casting other fields to varchar to prevent unknown type errors
    */
    select
          {{ cast_string_or_varchar('null') }} as claim_id
        , {{ cast_string_or_varchar('null') }} as claim_line_number
        , {{ cast_string_or_varchar('null') }} as patient_id
        , {{ cast_string_or_varchar('null') }} as member_id
        , {{ cast_string_or_varchar('null') }} as prescribing_provider_npi
        , {{ cast_string_or_varchar('null') }} as dispensing_provider_npi
        , cast(null as date) as dispensing_date
        , {{ cast_string_or_varchar('null') }} as ndc
        , {{ cast_string_or_varchar('null') }} as quantity
        , {{ cast_string_or_varchar('null') }} as days_supply
        , {{ cast_string_or_varchar('null') }} as refills
        , cast(null as date) as paid_date
        , {{ cast_string_or_varchar('null') }} as paid_amount
        , {{ cast_string_or_varchar('null') }} as allowed_amount
        , {{ cast_string_or_varchar('null') }} as data_source
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
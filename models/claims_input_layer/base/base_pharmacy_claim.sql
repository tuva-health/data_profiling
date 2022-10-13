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

pharmacy_claim_with_row_hash as (

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

)

select * from pharmacy_claim_with_row_hash
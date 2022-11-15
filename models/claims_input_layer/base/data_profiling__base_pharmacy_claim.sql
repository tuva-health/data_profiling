{{ config(
    enabled=var('data_profiling_enabled',var('tuva_packages_enabled',True))
) }}

-- depends on: {{ var('pharmacy_claim') }}

/*
    Not all data sources may exist. This block of code uses the relation_exists
    macro to check if a source exists. If the source does not exist it is logged
    and an empty pharmacy claim table is used instead.
*/


  {% set other_pharmacy_claim = {'exists': False, 'database': '', 'schema': '', 'alias': '' } %}
  {% for node in graph.nodes.values()
     |selectattr("resource_type", "equalto", "model")
     |selectattr("name", "equalto", "pharmacy_claim") %}


    {# |selectattr("package_name", "!=", "data_profiling")  dont need this anymore I dont think? #}


    {% do other_pharmacy_claim.update({'exists': True }) %}   {# obnoxious workaround.  varaibles set in a jinja for loop are local to the for loop, but dictionaries persist #}
    {% do other_pharmacy_claim.update({'database': node.database }) %}
    {% do other_pharmacy_claim.update({'schema': node.schema }) %}
    {% do other_pharmacy_claim.update({'alias': node.alias }) %}

  {% endfor %}


    {% set source_exists = (load_relation(source('claims_input','pharmacy_claim'))) is not none -%}
    {# {- log(other_pharmacy_claim, info=true) -} #}




with pharmacy_claim_src as (


    {% if project_name != 'data_profiling' and other_pharmacy_claim.exists  %}
    {% if execute%}{{- log("pharmacy claim reference exists.", info=true) -}}{% endif %}
    select * from {{other_pharmacy_claim.database}}.{{other_pharmacy_claim.schema}}.{{other_pharmacy_claim.alias}}


    {% elif project_name == 'data_profiling' and source_exists  %}
    {% if execute%}{{- log("pharmacy claim source exists.", info=true) -}}{% endif %}
    select * from {{source('claims_input','pharmacy_claim')}}


    {% else %}
    {% if project_name != 'data_profiling' and execute %}
    {{- log("pharmacy_claim ref does not exist, using empty table.", info=true) -}}
    {% endif %}

    {% if project_name == 'data_profiling' and execute %}
    {{- log("pharmacy_claim soruce does not exist, using empty table.", info=true) -}}
    {% endif %}




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
        , {{ cast_string_or_varchar('null') }} as ndc_code
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
                , 'ndc_code'
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
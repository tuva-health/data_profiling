{{ config(
    enabled=var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))
) }}

-- depends on: {{ var('eligibility') }}

/*
    Not all data sources may exist. This block of code uses the relation_exists
    macro to check if a source exists. If the source does not exist it is logged
    and an empty eligibility table is used instead.
*/

  {% set other_eligibility = {'exists': False, 'database': '', 'schema': '', 'alias': '' } %}
  {% for node in graph.nodes.values()
     |selectattr("resource_type", "equalto", "model")
     |selectattr("name", "equalto", "eligibility") %}


    {# |selectattr("package_name", "!=", "data_profiling")  dont need this anymore I dont think? #}


    {% do other_eligibility.update({'exists': True }) %}   {# obnoxious workaround.  varaibles set in a jinja for loop are local to the for loop, but dictionaries persist #}
    {% do other_eligibility.update({'database': node.database }) %}
    {% do other_eligibility.update({'schema': node.schema }) %}
    {% do other_eligibility.update({'alias': node.alias }) %}

  {% endfor %}


    {% set source_exists = (load_relation(source('claims_input','eligibility'))) is not none -%}
    {# {- log(other_eligibility, info=true) -} #}



with eligibility_src as (


    {% if project_name != 'data_profiling' and other_eligibility.exists  %}
    {% if execute%}{{- log("eligibility reference exists.", info=true) -}}{% endif %}
    select * from {{other_eligibility.database}}.{{other_eligibility.schema}}.{{other_eligibility.alias}}


    {% elif project_name == 'data_profiling' and source_exists  %}
    {% if execute%}{{- log("eligibility source exists.", info=true) -}}{% endif %}
    select * from {{source('claims_input','eligibility')}}


    {% else %}
    {% if project_name != 'data_profiling' and execute %}
    {{- log("eligibility ref does not exist, using empty table.", info=true) -}}
    {% endif %}

    {% if project_name == 'data_profiling' and execute %}
    {{- log("eligibility soruce does not exist, using empty table.", info=true) -}}
    {% endif %}



    /*
        casting fields used in joins and tests to correct data types
        casting other fields to varchar to prevent unknown type errors
    */
    select
          {{ cast_string_or_varchar('null') }} as patient_id
        , {{ cast_string_or_varchar('null') }} as member_id
        , {{ cast_string_or_varchar('null') }} as gender
        , {{ cast_string_or_varchar('null') }} as race
        , cast(null as date) as birth_date
        , cast(null as date) as death_date
        , {{ cast_string_or_varchar('null') }} as death_flag
        , cast(null as date) as enrollment_start_date
        , cast(null as date) as enrollment_end_date
        , {{ cast_string_or_varchar('null') }} as payer
        , {{ cast_string_or_varchar('null') }} as payer_type
        , {{ cast_string_or_varchar('null') }} as dual_status_code
        , {{ cast_string_or_varchar('null') }} as medicare_status_code
        , {{ cast_string_or_varchar('null') }} as first_name
        , {{ cast_string_or_varchar('null') }} as last_name
        , {{ cast_string_or_varchar('null') }} as address
        , {{ cast_string_or_varchar('null') }} as city
        , {{ cast_string_or_varchar('null') }} as state
        , {{ cast_string_or_varchar('null') }} as zip_code
        , {{ cast_string_or_varchar('null') }} as phone
        , {{ cast_string_or_varchar('null') }} as data_source
    limit 0

    {%- endif %}

),

eligibility_with_row_hash as (

    select *
         , {{ dbt_utils.surrogate_key([
                  'patient_id'
                , 'member_id'
                , 'gender'
                , 'race'
                , 'birth_date'
                , 'death_date'
                , 'death_flag'
                , 'enrollment_start_date'
                , 'enrollment_end_date'
                , 'payer'
                , 'payer_type'
                , 'dual_status_code'
                , 'medicare_status_code'
                , 'first_name'
                , 'last_name'
                , 'address'
                , 'city'
                , 'state'
                , 'zip_code'
                , 'phone'
                , 'data_source'
               ]) }}
           as row_hash
    from eligibility_src

)

select * from eligibility_with_row_hash
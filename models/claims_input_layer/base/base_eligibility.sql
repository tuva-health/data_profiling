/*
    Not all data sources may exist. This block of code uses the relation_exists
    macro to check if a source exists. If the source does not exist it is logged
    and an empty eligibility table is used instead.
*/
with eligibility_src as (

    {% set relation_exists = (load_relation(source(var('source_name'),'eligibility'))) is not none -%}

    {%- if relation_exists -%}
    {{- log("Eligibility source exists.", info=true) -}}

    select * from {{ var('eligibility') }}

    {%- else -%}
    {{- log("Eligibility source doesn't exist using an empty table instead.", info=true) -}}

    /*
        casting fields used in joins and tested to correct data types
        integer fields do not need casting
    */
    select
          {{ cast_string_or_varchar('null') }} as patient_id
        , {{ cast_string_or_varchar('null') }} as member_id
        , {{ cast_string_or_varchar('null') }} as gender
        , null as race
        , cast(null as date) as birth_date
        , cast(null as date) as death_date
        , null as death_flag
        , cast(null as date) as enrollment_start_date
        , cast(null as date) as enrollment_end_date
        , null as payer
        , null as payer_type
        , null as dual_status_code
        , null as medicare_status_code
        , null as first_name
        , null as last_name
        , null as address
        , null as city
        , null as state
        , null as zip_code
        , null as phone
        , null as data_source
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
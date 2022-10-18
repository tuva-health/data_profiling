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
    {{- log("Eligibility source doesn't exist using blank table instead.", info=true) -}}

    select
          null as patient_id
        , null as gender
        , null as birth_date
        , null as race
        , null as zip_code
        , null as state
        , null as deceased_flag
        , null as deceased_date
        , null as payer
        , null as payer_type
        , null as dual_status
        , null as medicare_status
        , null as month
        , null as year
    where false

    {%- endif %}

),

eligibility_with_row_hash as (

    select *
         , {{ dbt_utils.surrogate_key([
                 'patient_id'
               , 'gender'
               , 'birth_date'
               , 'race'
               , 'zip_code'
               , 'state'
               , 'deceased_flag'
               , 'deceased_date'
               , 'payer'
               , 'payer_type'
               , 'dual_status'
               , 'medicare_status'
               , 'month'
               , 'year'
               ]) }}
           as row_hash
    from eligibility_src

)

select * from eligibility_with_row_hash
with eligibility_src as (

    select * from {{ var('eligibility') }}

),

seed_gender as (

    select * from {{ ref('gender') }}

),

eligibility_with_row_key as (

    select *
         , {{ dbt_utils.surrogate_key([
                 'patient_id'
               , 'gender'
               , 'birth_date'
               , 'race'
               , 'zip_code'
               , 'state'
               , 'deceased_flag'
               , 'death_date'
               , 'payer'
               , 'payer_type'
               , 'dual_status'
               , 'medicare_status'
               , 'month'
               , 'year'
               ]) }}
           as row_hash
    from eligibility_src
),

duplicate_record as (

    select row_hash
    from eligibility_with_row_key
    group by row_hash
    having count (*) > 1

),

duplicate_patient_id as (

    select patient_id
    from (
        select distinct
              patient_id
            , payer
        from eligibility_with_row_key
    )
    group by patient_id
    having count(*) > 1

),

joined as (

    select
          eligibility_with_row_key.patient_id
        , eligibility_with_row_key.month
        , eligibility_with_row_key.year
        , case
            when duplicate_record.row_hash is null then 0
            else 1
          end as duplicate_record_elig
        , case
            when duplicate_patient_id.patient_id is null then 0
            else 1
          end as duplicate_patient_id_elig
        , {{ missing_field_check('eligibility_with_row_key.patient_id') }} as missing_patient_id_elig
        , {{ missing_field_check('eligibility_with_row_key.month') }} as missing_month_elig
        , {{ missing_field_check('eligibility_with_row_key.year') }} as missing_year_elig
        , {{ missing_field_check('eligibility_with_row_key.gender') }} as missing_gender_elig
        , {{ missing_field_check('eligibility_with_row_key.birth_date') }} as missing_birth_date_elig
        , {{ missing_field_check('eligibility_with_row_key.death_date') }} as missing_death_date_elig
        , {{ valid_past_or_current_date_check('eligibility_with_row_key.birth_date') }} as invalid_birth_date_elig
        , {{ valid_past_or_current_date_check('eligibility_with_row_key.death_date') }} as invalid_death_date_elig
        , case
            when eligibility_with_row_key.death_date is null then 0
            when eligibility_with_row_key.death_date is not null
              and eligibility_with_row_key.death_date > eligibility_with_row_key.birth_date
              then 0
            else 1
          end as invalid_death_before_birth_elig
        , case
            when eligibility_with_row_key.gender is null then 0
            when seed_gender.description is not null then 0
            else 1
          end as invalid_gender_elig
    from eligibility_with_row_key
         left join duplicate_record
            on eligibility_with_row_key.row_hash = duplicate_record.row_hash
         left join duplicate_patient_id
            on eligibility_with_row_key.patient_id = duplicate_patient_id.patient_id
         left join seed_gender
            on eligibility_with_row_key.gender = seed_gender.description

)

select
      patient_id
    , month
    , year
    , duplicate_record_elig
    , duplicate_patient_id_elig
    , missing_patient_id_elig
    , missing_month_elig
    , missing_year_elig
    , missing_gender_elig
    , missing_birth_date_elig
    , missing_death_date_elig
    , invalid_birth_date_elig
    , invalid_death_date_elig
    , invalid_death_before_birth_elig
    , invalid_gender_elig
    , {{ current_date_or_timestamp('timestamp') }} as run_date
from joined
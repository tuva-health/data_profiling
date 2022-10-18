with eligibility as (

    select * from {{ ref('base_eligibility') }}

),

seed_gender as (

    select * from {{ ref('gender') }}

),

duplicate_record as (

    select row_hash
    from eligibility
    group by row_hash
    having count (*) > 1

),

duplicate_patient_id as (

    select distinct patient_id
    from (
        select
              patient_id
            , month
            , year
            , payer
            , payer_type
        from eligibility
        group by
              patient_id
            , month
            , year
            , payer
            , payer_type
        having count(*) > 1
    )

),

joined as (

    select
          eligibility.patient_id
        , eligibility.month
        , eligibility.year
        , eligibility.payer
        , eligibility.payer_type
        , case
            when duplicate_record.row_hash is null then 0
            else 1
          end as duplicate_record_elig
        , case
            when duplicate_patient_id.patient_id is null then 0
            else 1
          end as duplicate_patient_id_elig
        , {{ missing_field_check('eligibility.patient_id') }} as missing_patient_id_elig
        , {{ missing_field_check('eligibility.month') }} as missing_month_elig
        , {{ missing_field_check('eligibility.year') }} as missing_year_elig
        , {{ missing_field_check('eligibility.gender') }} as missing_gender_elig
        , {{ missing_field_check('eligibility.birth_date') }} as missing_birth_date_elig
        , {{ missing_field_check('eligibility.deceased_date') }} as missing_deceased_date_elig
        , {{ valid_birth_or_death_date_check('eligibility.birth_date') }} as invalid_birth_date_elig
        , {{ valid_birth_or_death_date_check('eligibility.deceased_date') }} as invalid_deceased_date_elig
        , case
            when eligibility.deceased_date is null then 0
            when eligibility.deceased_date is not null
              and eligibility.deceased_date > eligibility.birth_date
              then 0
            else 1
          end as invalid_death_before_birth_elig
        , case
            when eligibility.gender is null then 0
            when seed_gender.description is not null then 0
            else 1
          end as invalid_gender_elig
    from eligibility
         left join duplicate_record
            on eligibility.row_hash = duplicate_record.row_hash
         left join duplicate_patient_id
            on eligibility.patient_id = duplicate_patient_id.patient_id
         left join seed_gender
            on eligibility.gender = seed_gender.description

)

/* casting fields used as unique key in snapshot */
select
      {{ cast_string_or_varchar('patient_id') }} as patient_id
    , {{ cast_string_or_varchar('month') }} as month
    , {{ cast_string_or_varchar('year') }} as year
    , {{ cast_string_or_varchar('payer') }} as payer
    , {{ cast_string_or_varchar('payer_type') }} as payer_type
    , duplicate_record_elig
    , duplicate_patient_id_elig
    , missing_patient_id_elig
    , missing_month_elig
    , missing_year_elig
    , missing_gender_elig
    , missing_birth_date_elig
    , missing_deceased_date_elig
    , invalid_birth_date_elig
    , invalid_deceased_date_elig
    , invalid_death_before_birth_elig
    , invalid_gender_elig
    , {{ current_date_or_timestamp('timestamp') }} as run_date
from joined
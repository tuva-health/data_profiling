with eligibility as (

    select * from {{ ref('base_eligibility') }}

),

seed_gender as (

    select * from {{ ref('gender') }}

),

deaths_from_claims as (

    select distinct patient_id
    from {{ ref('base_medical_claim') }}
    where discharge_disposition_code  = '20'

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
            , member_id
            , enrollment_start_date
            , enrollment_end_date
            , payer
            , payer_type
        from eligibility
        group by
              patient_id
            , member_id
            , enrollment_start_date
            , enrollment_end_date
            , payer
            , payer_type
        having count(*) > 1
    )

),

joined as (

    select
          eligibility.patient_id
        , eligibility.member_id
        , eligibility.enrollment_start_date
        , eligibility.enrollment_end_date
        , eligibility.payer
        , eligibility.payer_type
        , case
            when duplicate_record.row_hash is null then 0
            else 1
          end as duplicate_eligibility_record
        , case
            when duplicate_patient_id.patient_id is null then 0
            else 1
          end as duplicate_patient_id
        , {{ missing_field_check('eligibility.patient_id') }} as missing_eligibility_patient_id
        , {{ missing_field_check('eligibility.member_id') }} as missing_eligibility_member_id
        , {{ missing_field_check('eligibility.enrollment_start_date') }} as missing_enrollment_start_date
        , {{ valid_claim_date_check('eligibility.enrollment_start_date') }} as invalid_enrollment_start_date
        , {{ missing_field_check('eligibility.enrollment_end_date') }} as missing_enrollment_end_date
        , {{ valid_claim_date_check('eligibility.enrollment_end_date') }} as invalid_enrollment_end_date
        , case
            when eligibility.enrollment_end_date is null then 0
            when eligibility.enrollment_end_date is not null
              and eligibility.enrollment_end_date > eligibility.enrollment_start_date
              then 0
            else 1
          end as invalid_enrollment_end_before_start
        , {{ missing_field_check('eligibility.birth_date') }} as missing_birth_date
        , {{ valid_birth_or_death_date_check('eligibility.birth_date') }} as invalid_birth_date
        , case
            when eligibility.death_date is null
              and eligibility.death_flag is not null
              then 1
            when eligibility.death_date is null
              and deaths_from_claims.patient_id is not null
              then 1
            else 0
          end as missing_death_date
        , {{ valid_birth_or_death_date_check('eligibility.death_date') }} as invalid_death_date
        , case
            when eligibility.death_date is null then 0
            when eligibility.death_date is not null
              and eligibility.death_date > eligibility.birth_date
              then 0
            else 1
          end as invalid_death_before_birth
        , {{ missing_field_check('eligibility.gender') }} as missing_gender
        , case
            when eligibility.gender is null then 0
            when seed_gender.description is not null then 0
            else 1
          end as invalid_gender
    from eligibility
         left join duplicate_record
            on eligibility.row_hash = duplicate_record.row_hash
         left join duplicate_patient_id
            on eligibility.patient_id = duplicate_patient_id.patient_id
         left join seed_gender
            on eligibility.gender = seed_gender.description
         left join deaths_from_claims
            on eligibility.patient_id = deaths_from_claims.patient_id

)

/* casting fields used as unique key in snapshot */
select
      {{ cast_string_or_varchar('patient_id') }} as patient_id
    , {{ cast_string_or_varchar('member_id') }} as member_id
    , {{ cast_string_or_varchar('enrollment_start_date') }} as enrollment_start_date
    , {{ cast_string_or_varchar('enrollment_end_date') }} as enrollment_end_date
    , {{ cast_string_or_varchar('payer') }} as payer
    , {{ cast_string_or_varchar('payer_type') }} as payer_type
    , duplicate_eligibility_record
    , duplicate_patient_id
    , missing_eligibility_patient_id
    , missing_eligibility_member_id
    , missing_enrollment_start_date
    , invalid_enrollment_start_date
    , missing_enrollment_end_date
    , invalid_enrollment_end_date
    , invalid_enrollment_end_before_start
    , missing_birth_date
    , invalid_birth_date
    , missing_death_date
    , invalid_death_date
    , invalid_death_before_birth
    , missing_gender
    , invalid_gender
    , {{ current_date_or_timestamp('timestamp') }} as run_date
from joined
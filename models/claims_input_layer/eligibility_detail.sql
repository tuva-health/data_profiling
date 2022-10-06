with eligibility_src as (

    select * from {{ var('eligibility') }}

),

seed_gender as (

    select * from {{ ref('gender') }}

),

duplicate_record as (

    select
          patient_id
        , month
        , year
    from (
        select
              patient_id
            , gender
            , birth_date
            , race
            , zip_code
            , state
            , deceased_flag
            , death_date
            , payer
            , payer_type
            , dual_status
            , medicare_status
            , month
            , year
        from eligibility_src
        group by
              patient_id
            , gender
            , birth_date
            , race
            , zip_code
            , state
            , deceased_flag
            , death_date
            , payer
            , payer_type
            , dual_status
            , medicare_status
            , month
            , year
        having count (*) > 1
    )

),

duplicate_patient_id as (

    select patient_id
    from (
        select distinct
              patient_id
            , payer
        from eligibility_src
    )
    group by patient_id
    having count(*) > 1

),

joined as (

    select
          eligibility_src.patient_id
        , eligibility_src.month
        , eligibility_src.year
        , case
            when duplicate_record.patient_id is null then 0
            else 1
          end as test_0001 /*duplicate_record_flag*/
        , case
            when duplicate_patient_id.patient_id is null then 0
            else 1
          end as test_0002	/*duplicate_patient_id_flag*/
        , {{ missing_field_check('eligibility_src.patient_id') }} as test_0003 /*missing_patient_id_flag*/
        , {{ missing_field_check('eligibility_src.month') }} as test_0004 /*missing_month_flag*/
        , {{ missing_field_check('eligibility_src.year') }} as test_0005 /*missing_year_flag*/
        , {{ missing_field_check('eligibility_src.gender') }} as test_0006 /*missing_gender_flag*/
        , {{ missing_field_check('eligibility_src.birth_date') }} as test_0007 /*missing_birth_date_flag*/
        , {{ missing_field_check('eligibility_src.death_date') }} as test_0008 /*missing_death_date_flag*/
        , {{ valid_past_or_current_date_check('eligibility_src.birth_date') }} as test_0009 /*invalid_birth_date_flag*/
        , {{ valid_past_or_current_date_check('eligibility_src.death_date') }} as test_0010 /*invalid_death_date_flag*/
        , case
            when eligibility_src.death_date is null then 0
            when eligibility_src.death_date is not null
              and eligibility_src.death_date > eligibility_src.birth_date
              then 0
            else 1
          end as test_0011 /*invalid_death_before_birth_flag*/
        , case
            when eligibility_src.gender is null then 0
            when seed_gender.description is not null then 0
            else 1
          end as test_0012 /*invalid_gender_flag*/
    from eligibility_src
         left join duplicate_record
            on eligibility_src.patient_id = duplicate_record.patient_id
            and eligibility_src.month = duplicate_record.month
            and eligibility_src.year = duplicate_record.year
         left join duplicate_patient_id
            on eligibility_src.patient_id = duplicate_patient_id.patient_id
         left join seed_gender
            on eligibility_src.gender = seed_gender.description

)

select
      patient_id
    , month
    , year
    , test_0001 /*duplicate_record_flag*/
    , test_0002 /*duplicate_patient_id_flag*/
    , test_0003 /*missing_patient_id_flag*/
    , test_0004 /*missing_month_flag*/
    , test_0005 /*missing_year_flag*/
    , test_0006 /*missing_gender_flag*/
    , test_0007 /*missing_birth_date_flag*/
    , test_0008 /*missing_death_date_flag*/
    , test_0009 /*invalid_birth_date_flag*/
    , test_0010 /*invalid_death_date_flag*/
    , test_0011 /*invalid_death_before_birth_flag*/
    , test_0012 /*invalid_gender_flag*/
    , {{ current_date_or_timestamp('timestamp') }} as run_date
from joined
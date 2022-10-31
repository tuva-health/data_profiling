{% snapshot snapshot_eligibility_detail %}

{{
    config(
        target_database = var('output_database')
      , target_schema = var('output_schema')
      , strategy = 'timestamp'
      , updated_at = 'run_date'
      , unique_key = 'patient_id||member_id||enrollment_start_date||enrollment_end_date||payer||payer_type||run_date'
    )
}}

select * from {{ ref('eligibility_detail') }}

{% endsnapshot %}
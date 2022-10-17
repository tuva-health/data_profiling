{% snapshot snapshot_eligibility_detail %}

{{
    config(
        target_database = var('output_database')
      , target_schema = var('output_schema')
      , strategy = 'timestamp'
      , updated_at = 'run_date'
      , unique_key = "patient_id||'-'||month||'-'||year||''-''||payer||''-''||payer_type"
    )
}}

select * from {{ ref('eligibility_detail') }}

{% endsnapshot %}
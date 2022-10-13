{% snapshot snapshot_claim_summary %}

{{
    config(
        target_database = var('output_database')
      , target_schema = var('output_schema')
      , strategy = 'timestamp'
      , updated_at = 'run_date'
      , unique_key = 'test_name'
    )
}}

select * from {{ ref('claim_summary') }}

{% endsnapshot %}
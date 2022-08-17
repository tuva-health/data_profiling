--todo: remove enabled flag

{% snapshot snapshot_claim_summary %}

{{
    config(
        enabled = false
      , target_database = var('output_database')
      , target_schema = var('output_schema')
      , strategy = 'timestamp'
      , updated_at = 'run_date'
      , unique_key = "table_name||'-'||column_name"
    )
}}

select * from {{ ref('claim_summary') }}

{% endsnapshot %}
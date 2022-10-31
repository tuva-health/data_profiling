{% snapshot snapshot_medical_claim_detail %}

{{
    config(
        target_database = var('output_database')
      , target_schema = var('output_schema')
      , strategy = 'timestamp'
      , updated_at = 'run_date'
      , unique_key = 'claim_id||claim_line_number||run_date'
    )
}}

select * from {{ ref('medical_claim_detail') }}

{% endsnapshot %}
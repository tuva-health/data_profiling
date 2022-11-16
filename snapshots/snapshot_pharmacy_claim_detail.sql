{% snapshot snapshot_pharmacy_claim_detail %}

{%- if (var('data_profiling_schema',None) != None or (var('data_profiling_schema',None) == None and var('tuva_schema_prefix',None) == None))  -%}
    {{ config(
        target_database = var('data_profiling_database',var('tuva_database','tuva'))
      , target_schema = var('data_profiling_schema','data_profiling')
      , strategy = 'timestamp'
      , updated_at = 'run_date'
      , unique_key = 'claim_id||claim_line_number||run_date'
      , enabled = var('data_profiling_enabled',var('tuva_packages_enabled',True))
      , tags= 'data_profiling'
    ) }}
{%- elif var('tuva_schema_prefix',None) != None -%}
    {{ config(
        target_database = var('data_profiling_database',var('tuva_database','tuva'))
      , target_schema = var('tuva_schema_prefix')~'_data_profiling'
      , strategy = 'timestamp'
      , updated_at = 'run_date'
      , unique_key = 'claim_id||claim_line_number||run_date'
      , enabled = var('data_profiling_enabled',var('tuva_packages_enabled',True))
      , tags= 'data_profiling'
    ) }}
{%- endif -%}


select * from {{ ref('data_profiling__pharmacy_claim_detail') }}

{% endsnapshot %}
{% snapshot snapshot_eligibility_detail %}

{%- if (var('data_profiling_schema',None) != None or (var('data_profiling_schema',None) == None and var('tuva_schema_prefix',None) == None))  -%}
    {{ config(
        target_database = var('data_profiling_database',var('tuva_database','tuva'))
      , target_schema = var('data_profiling_schema','data_profiling')
      , strategy = 'timestamp'
      , updated_at = 'run_date'
      , unique_key = 'patient_id||member_id||enrollment_start_date||enrollment_end_date||payer||payer_type||run_date'
      , enabled = var('data_profiling_enabled',var('tuva_packages_enabled',True))
      , tags= 'data_profiling'
    ) }}
{%- elif var('tuva_schema_prefix',None) != None -%}
    {{ config(
        target_database = var('data_profiling_database',var('tuva_database','tuva'))
      , target_schema = var('tuva_schema_prefix')~'_data_profiling'
      , strategy = 'timestamp'
      , updated_at = 'run_date'
      , unique_key = 'patient_id||member_id||enrollment_start_date||enrollment_end_date||payer||payer_type||run_date'
      , enabled = var('data_profiling_enabled',var('tuva_packages_enabled',True))
      , tags= 'data_profiling'
    ) }}
{%- endif -%}


select * from {{ ref('data_profiling__eligibility_detail') }}

{% endsnapshot %}
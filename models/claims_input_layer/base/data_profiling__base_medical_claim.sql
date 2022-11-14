{{ config(
    enabled=var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))
) }}

/*
    Not all data sources may exist. This block of code uses the relation_exists
    macro to check if a source exists. If the source does not exist it is logged
    and an empty medical claim table is used instead.
*/



  {% set other_medical_claim = {'exists': False, 'database': '', 'schema': '', 'alias': '' } %}
  {% for node in graph.nodes.values()
     |selectattr("resource_type", "equalto", "model")
     |selectattr("name", "equalto", "meical_Claim") %}


    {# |selectattr("package_name", "!=", "data_profiling")  dont need this anymore I dont think? #}


    {% do other_medical_claim.update({'exists': True }) %}   {# obnoxious workaround.  varaibles set in a jinja for loop are local to the for loop, but dictionaries persist #}
    {% do other_medical_claim.update({'database': node.database }) %}
    {% do other_medical_claim.update({'schema': node.schema }) %}
    {% do other_medical_claim.update({'alias': node.alias }) %}

  {% endfor %}


    {% set source_exists = (load_relation(source('claims_input','medical_claim'))) is not none -%}
    {# {- log(other_medical_claim, info=true) -} #}



with medical_claim_src as (


    {% if project_name != 'data_profiling' and other_medical_claim.exists  %}
    {% if execute%}{{- log("medical claim reference exists.", info=true) -}}{% endif %}
    select * from {{other_medical_claim.database}}.{{other_medical_claim.schema}}.{{other_medical_claim.alias}}


    {% elif project_name == 'data_profiling' and source_exists  %}
    {% if execute%}{{- log("medical claim source exists.", info=true) -}}{% endif %}
    select * from {{source('claims_input','medical_claim')}}


    {% else %}
    {% if project_name != 'data_profiling' and execute %}
    {{- log("medical claim ref does not exist, using empty table.", info=true) -}}
    {% endif %}

    {% if project_name == 'data_profiling' and execute %}
    {{- log("medical claim soruce does not exist, using empty table.", info=true) -}}
    {% endif %}


    /*
        casting fields used in joins and tests to correct data types
        casting other fields to varchar to prevent unknown type errors
    */
    select
          {{ cast_string_or_varchar('null') }} as claim_id
        , {{ cast_string_or_varchar('null') }} as claim_line_number
        , {{ cast_string_or_varchar('null') }} as claim_type
        , {{ cast_string_or_varchar('null') }} as patient_id
        , {{ cast_string_or_varchar('null') }} as member_id
        , cast(null as date) as claim_start_date
        , cast(null as date) as claim_end_date
        , cast(null as date) as claim_line_start_date
        , cast(null as date) as claim_line_end_date
        , cast(null as date) as admission_date
        , cast(null as date) as discharge_date
        , {{ cast_string_or_varchar('null') }} as admit_source_code
        , {{ cast_string_or_varchar('null') }} as admit_type_code
        , {{ cast_string_or_varchar('null') }} as discharge_disposition_code
        , {{ cast_string_or_varchar('null') }} as place_of_service_code
        , {{ cast_string_or_varchar('null') }} as bill_type_code
        , {{ cast_string_or_varchar('null') }} as ms_drg_code
        , {{ cast_string_or_varchar('null') }} as revenue_center_code
        , {{ cast_string_or_varchar('null') }} as service_unit_quantity
        , {{ cast_string_or_varchar('null') }} as hcpcs_code
        , {{ cast_string_or_varchar('null') }} as hcpcs_modifier_1
        , {{ cast_string_or_varchar('null') }} as hcpcs_modifier_2
        , {{ cast_string_or_varchar('null') }} as hcpcs_modifier_3
        , {{ cast_string_or_varchar('null') }} as hcpcs_modifier_4
        , {{ cast_string_or_varchar('null') }} as hcpcs_modifier_5
        , {{ cast_string_or_varchar('null') }} as rendering_npi
        , {{ cast_string_or_varchar('null') }} as billing_npi
        , {{ cast_string_or_varchar('null') }} as facility_npi
        , cast(null as date) as paid_date
        , {{ cast_string_or_varchar('null') }} as paid_amount
        , {{ cast_string_or_varchar('null') }} as allowed_amount
        , {{ cast_string_or_varchar('null') }} as charge_amount
        , {{ cast_string_or_varchar('null') }} as diagnosis_code_type
        , {{ cast_string_or_varchar('null') }} as diagnosis_code_1
        , {{ cast_string_or_varchar('null') }} as diagnosis_code_2
        , {{ cast_string_or_varchar('null') }} as diagnosis_code_3
        , {{ cast_string_or_varchar('null') }} as diagnosis_code_4
        , {{ cast_string_or_varchar('null') }} as diagnosis_code_5
        , {{ cast_string_or_varchar('null') }} as diagnosis_code_6
        , {{ cast_string_or_varchar('null') }} as diagnosis_code_7
        , {{ cast_string_or_varchar('null') }} as diagnosis_code_8
        , {{ cast_string_or_varchar('null') }} as diagnosis_code_9
        , {{ cast_string_or_varchar('null') }} as diagnosis_code_10
        , {{ cast_string_or_varchar('null') }} as diagnosis_code_11
        , {{ cast_string_or_varchar('null') }} as diagnosis_code_12
        , {{ cast_string_or_varchar('null') }} as diagnosis_code_13
        , {{ cast_string_or_varchar('null') }} as diagnosis_code_14
        , {{ cast_string_or_varchar('null') }} as diagnosis_code_15
        , {{ cast_string_or_varchar('null') }} as diagnosis_code_16
        , {{ cast_string_or_varchar('null') }} as diagnosis_code_17
        , {{ cast_string_or_varchar('null') }} as diagnosis_code_18
        , {{ cast_string_or_varchar('null') }} as diagnosis_code_19
        , {{ cast_string_or_varchar('null') }} as diagnosis_code_20
        , {{ cast_string_or_varchar('null') }} as diagnosis_code_21
        , {{ cast_string_or_varchar('null') }} as diagnosis_code_22
        , {{ cast_string_or_varchar('null') }} as diagnosis_code_23
        , {{ cast_string_or_varchar('null') }} as diagnosis_code_24
        , {{ cast_string_or_varchar('null') }} as diagnosis_code_25
        , {{ cast_string_or_varchar('null') }} as diagnosis_poa_1
        , {{ cast_string_or_varchar('null') }} as diagnosis_poa_2
        , {{ cast_string_or_varchar('null') }} as diagnosis_poa_3
        , {{ cast_string_or_varchar('null') }} as diagnosis_poa_4
        , {{ cast_string_or_varchar('null') }} as diagnosis_poa_5
        , {{ cast_string_or_varchar('null') }} as diagnosis_poa_6
        , {{ cast_string_or_varchar('null') }} as diagnosis_poa_7
        , {{ cast_string_or_varchar('null') }} as diagnosis_poa_8
        , {{ cast_string_or_varchar('null') }} as diagnosis_poa_9
        , {{ cast_string_or_varchar('null') }} as diagnosis_poa_10
        , {{ cast_string_or_varchar('null') }} as diagnosis_poa_11
        , {{ cast_string_or_varchar('null') }} as diagnosis_poa_12
        , {{ cast_string_or_varchar('null') }} as diagnosis_poa_13
        , {{ cast_string_or_varchar('null') }} as diagnosis_poa_14
        , {{ cast_string_or_varchar('null') }} as diagnosis_poa_15
        , {{ cast_string_or_varchar('null') }} as diagnosis_poa_16
        , {{ cast_string_or_varchar('null') }} as diagnosis_poa_17
        , {{ cast_string_or_varchar('null') }} as diagnosis_poa_18
        , {{ cast_string_or_varchar('null') }} as diagnosis_poa_19
        , {{ cast_string_or_varchar('null') }} as diagnosis_poa_20
        , {{ cast_string_or_varchar('null') }} as diagnosis_poa_21
        , {{ cast_string_or_varchar('null') }} as diagnosis_poa_22
        , {{ cast_string_or_varchar('null') }} as diagnosis_poa_23
        , {{ cast_string_or_varchar('null') }} as diagnosis_poa_24
        , {{ cast_string_or_varchar('null') }} as diagnosis_poa_25
        , {{ cast_string_or_varchar('null') }} as procedure_code_type
        , {{ cast_string_or_varchar('null') }} as procedure_code_1
        , {{ cast_string_or_varchar('null') }} as procedure_code_2
        , {{ cast_string_or_varchar('null') }} as procedure_code_3
        , {{ cast_string_or_varchar('null') }} as procedure_code_4
        , {{ cast_string_or_varchar('null') }} as procedure_code_5
        , {{ cast_string_or_varchar('null') }} as procedure_code_6
        , {{ cast_string_or_varchar('null') }} as procedure_code_7
        , {{ cast_string_or_varchar('null') }} as procedure_code_8
        , {{ cast_string_or_varchar('null') }} as procedure_code_9
        , {{ cast_string_or_varchar('null') }} as procedure_code_10
        , {{ cast_string_or_varchar('null') }} as procedure_code_11
        , {{ cast_string_or_varchar('null') }} as procedure_code_12
        , {{ cast_string_or_varchar('null') }} as procedure_code_13
        , {{ cast_string_or_varchar('null') }} as procedure_code_14
        , {{ cast_string_or_varchar('null') }} as procedure_code_15
        , {{ cast_string_or_varchar('null') }} as procedure_code_16
        , {{ cast_string_or_varchar('null') }} as procedure_code_17
        , {{ cast_string_or_varchar('null') }} as procedure_code_18
        , {{ cast_string_or_varchar('null') }} as procedure_code_19
        , {{ cast_string_or_varchar('null') }} as procedure_code_20
        , {{ cast_string_or_varchar('null') }} as procedure_code_21
        , {{ cast_string_or_varchar('null') }} as procedure_code_22
        , {{ cast_string_or_varchar('null') }} as procedure_code_23
        , {{ cast_string_or_varchar('null') }} as procedure_code_24
        , {{ cast_string_or_varchar('null') }} as procedure_code_25
        , {{ cast_string_or_varchar('null') }} as procedure_date_1
        , {{ cast_string_or_varchar('null') }} as procedure_date_2
        , {{ cast_string_or_varchar('null') }} as procedure_date_3
        , {{ cast_string_or_varchar('null') }} as procedure_date_4
        , {{ cast_string_or_varchar('null') }} as procedure_date_5
        , {{ cast_string_or_varchar('null') }} as procedure_date_6
        , {{ cast_string_or_varchar('null') }} as procedure_date_7
        , {{ cast_string_or_varchar('null') }} as procedure_date_8
        , {{ cast_string_or_varchar('null') }} as procedure_date_9
        , {{ cast_string_or_varchar('null') }} as procedure_date_10
        , {{ cast_string_or_varchar('null') }} as procedure_date_11
        , {{ cast_string_or_varchar('null') }} as procedure_date_12
        , {{ cast_string_or_varchar('null') }} as procedure_date_13
        , {{ cast_string_or_varchar('null') }} as procedure_date_14
        , {{ cast_string_or_varchar('null') }} as procedure_date_15
        , {{ cast_string_or_varchar('null') }} as procedure_date_16
        , {{ cast_string_or_varchar('null') }} as procedure_date_17
        , {{ cast_string_or_varchar('null') }} as procedure_date_18
        , {{ cast_string_or_varchar('null') }} as procedure_date_19
        , {{ cast_string_or_varchar('null') }} as procedure_date_20
        , {{ cast_string_or_varchar('null') }} as procedure_date_21
        , {{ cast_string_or_varchar('null') }} as procedure_date_22
        , {{ cast_string_or_varchar('null') }} as procedure_date_23
        , {{ cast_string_or_varchar('null') }} as procedure_date_24
        , {{ cast_string_or_varchar('null') }} as procedure_date_25
        , {{ cast_string_or_varchar('null') }} as data_source
    limit 0

    {%- endif %}

),

medical_claim_with_row_hash as (

    select *
         , {{ dbt_utils.surrogate_key([
                 'claim_id', 'claim_line_number', 'claim_type'
               , 'patient_id', 'member_id', 'claim_start_date'
               , 'claim_end_date', 'claim_line_start_date'
               , 'claim_line_end_date', 'admission_date', 'discharge_date'
               , 'admit_source_code', 'admit_type_code'
               , 'discharge_disposition_code', 'place_of_service_code'
               , 'bill_type_code', 'ms_drg_code', 'revenue_center_code'
               , 'service_unit_quantity', 'hcpcs_code', 'hcpcs_modifier_1'
               , 'hcpcs_modifier_2', 'hcpcs_modifier_3', 'hcpcs_modifier_4'
               , 'hcpcs_modifier_5', 'rendering_npi', 'billing_npi'
               , 'facility_npi', 'paid_date', 'paid_amount'
               , 'allowed_amount', 'charge_amount', 'diagnosis_code_type'
               , 'diagnosis_code_1', 'diagnosis_code_2', 'diagnosis_code_3'
               , 'diagnosis_code_4', 'diagnosis_code_5', 'diagnosis_code_6'
               , 'diagnosis_code_7', 'diagnosis_code_8', 'diagnosis_code_9'
               , 'diagnosis_code_10', 'diagnosis_code_11', 'diagnosis_code_12'
               , 'diagnosis_code_13', 'diagnosis_code_14', 'diagnosis_code_15'
               , 'diagnosis_code_16', 'diagnosis_code_17', 'diagnosis_code_18'
               , 'diagnosis_code_19', 'diagnosis_code_20', 'diagnosis_code_21'
               , 'diagnosis_code_22', 'diagnosis_code_23', 'diagnosis_code_24'
               , 'diagnosis_code_25', 'diagnosis_poa_1', 'diagnosis_poa_2'
               , 'diagnosis_poa_3', 'diagnosis_poa_4', 'diagnosis_poa_5'
               , 'diagnosis_poa_6', 'diagnosis_poa_7', 'diagnosis_poa_8'
               , 'diagnosis_poa_9', 'diagnosis_poa_10', 'diagnosis_poa_11'
               , 'diagnosis_poa_12', 'diagnosis_poa_13', 'diagnosis_poa_14'
               , 'diagnosis_poa_15', 'diagnosis_poa_16', 'diagnosis_poa_17'
               , 'diagnosis_poa_18', 'diagnosis_poa_19', 'diagnosis_poa_20'
               , 'diagnosis_poa_21', 'diagnosis_poa_22', 'diagnosis_poa_23'
               , 'diagnosis_poa_24', 'diagnosis_poa_25', 'procedure_code_type'
               , 'procedure_code_1', 'procedure_code_2', 'procedure_code_3'
               , 'procedure_code_4', 'procedure_code_5', 'procedure_code_6'
               , 'procedure_code_7', 'procedure_code_8', 'procedure_code_9'
               , 'procedure_code_10', 'procedure_code_11', 'procedure_code_12'
               , 'procedure_code_13', 'procedure_code_14', 'procedure_code_15'
               , 'procedure_code_16', 'procedure_code_17', 'procedure_code_18'
               , 'procedure_code_19', 'procedure_code_20', 'procedure_code_21'
               , 'procedure_code_22', 'procedure_code_23', 'procedure_code_24'
               , 'procedure_code_25', 'procedure_date_1', 'procedure_date_2'
               , 'procedure_date_3', 'procedure_date_4', 'procedure_date_5'
               , 'procedure_date_6', 'procedure_date_7', 'procedure_date_8'
               , 'procedure_date_9', 'procedure_date_10', 'procedure_date_11'
               , 'procedure_date_12', 'procedure_date_13', 'procedure_date_14'
               , 'procedure_date_15', 'procedure_date_16', 'procedure_date_17'
               , 'procedure_date_18', 'procedure_date_19', 'procedure_date_20'
               , 'procedure_date_21', 'procedure_date_22', 'procedure_date_23'
               , 'procedure_date_24', 'procedure_date_25', 'data_source'
               ]) }}
           as row_hash
    from medical_claim_src

)

select * from medical_claim_with_row_hash
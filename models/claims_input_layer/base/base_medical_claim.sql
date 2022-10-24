/*
    Not all data sources may exist. This block of code uses the relation_exists
    macro to check if a source exists. If the source does not exist it is logged
    and an empty medical claim table is used instead.
*/
with medical_claim_src as (

    {% set relation_exists = (load_relation(source(var('source_name'),'medical_claim'))) is not none -%}

    {%- if relation_exists -%}
    {{- log("Medical claim source exists.", info=true) -}}

    select * from {{ var('medical_claim') }}

    {%- else -%}
    {{- log("Medical claim source doesn't exist using blank table instead.", info=true) -}}

    select
          'blank' as claim_id
        , 'blank' as claim_line_number
        , 'blank' as patient_id
        , cast(null as date) as claim_start_date
        , cast(null as date) as claim_end_date
        , cast(null as date) as claim_line_start_date
        , cast(null as date) as claim_line_end_date
        , cast(null as date) as admission_date
        , cast(null as date) as discharge_date
        , null as claim_type
        , null as bill_type_code
        , null as place_of_service_code
        , null as admit_source_code
        , null as admit_type_code
        , null as discharge_disposition_code
        , null as ms_drg
        , null as revenue_center_code
        , null as hcpcs_code
        , null as hcpcs_modifier_1
        , null as hcpcs_modifier_2
        , null as hcpcs_modifier_3
        , null as hcpcs_modifier_4
        , null as hcpcs_modifier_5
        , null as billing_npi
        , null as rendering_npi
        , null as facility_npi
        , cast(null as date) as paid_date
        , null as paid_amount
        , null as charge_amount
        , null as diagnosis_code_1
        , null as diagnosis_code_2
        , null as diagnosis_code_3
        , null as diagnosis_code_4
        , null as diagnosis_code_5
        , null as diagnosis_code_6
        , null as diagnosis_code_7
        , null as diagnosis_code_8
        , null as diagnosis_code_9
        , null as diagnosis_code_10
        , null as diagnosis_code_11
        , null as diagnosis_code_12
        , null as diagnosis_code_13
        , null as diagnosis_code_14
        , null as diagnosis_code_15
        , null as diagnosis_code_16
        , null as diagnosis_code_17
        , null as diagnosis_code_18
        , null as diagnosis_code_19
        , null as diagnosis_code_20
        , null as diagnosis_code_21
        , null as diagnosis_code_22
        , null as diagnosis_code_23
        , null as diagnosis_code_24
        , null as diagnosis_code_25
        , null as diagnosis_poa_1
        , null as diagnosis_poa_2
        , null as diagnosis_poa_3
        , null as diagnosis_poa_4
        , null as diagnosis_poa_5
        , null as diagnosis_poa_6
        , null as diagnosis_poa_7
        , null as diagnosis_poa_8
        , null as diagnosis_poa_9
        , null as diagnosis_poa_10
        , null as diagnosis_poa_11
        , null as diagnosis_poa_12
        , null as diagnosis_poa_13
        , null as diagnosis_poa_14
        , null as diagnosis_poa_15
        , null as diagnosis_poa_16
        , null as diagnosis_poa_17
        , null as diagnosis_poa_18
        , null as diagnosis_poa_19
        , null as diagnosis_poa_20
        , null as diagnosis_poa_21
        , null as diagnosis_poa_22
        , null as diagnosis_poa_23
        , null as diagnosis_poa_24
        , null as diagnosis_poa_25
        , null as diagnosis_code_type
        , null as procedure_code_type
        , null as procedure_code_1
        , null as procedure_code_2
        , null as procedure_code_3
        , null as procedure_code_4
        , null as procedure_code_5
        , null as procedure_code_6
        , null as procedure_code_7
        , null as procedure_code_8
        , null as procedure_code_9
        , null as procedure_code_10
        , null as procedure_code_11
        , null as procedure_code_12
        , null as procedure_code_13
        , null as procedure_code_14
        , null as procedure_code_15
        , null as procedure_code_16
        , null as procedure_code_17
        , null as procedure_code_18
        , null as procedure_code_19
        , null as procedure_code_20
        , null as procedure_code_21
        , null as procedure_code_22
        , null as procedure_code_23
        , null as procedure_code_24
        , null as procedure_code_25
        , cast(null as date) as procedure_date_1
        , cast(null as date) as procedure_date_2
        , cast(null as date) as procedure_date_3
        , cast(null as date) as procedure_date_4
        , cast(null as date) as procedure_date_5
        , cast(null as date) as procedure_date_6
        , cast(null as date) as procedure_date_7
        , cast(null as date) as procedure_date_8
        , cast(null as date) as procedure_date_9
        , cast(null as date) as procedure_date_10
        , cast(null as date) as procedure_date_11
        , cast(null as date) as procedure_date_12
        , cast(null as date) as procedure_date_13
        , cast(null as date) as procedure_date_14
        , cast(null as date) as procedure_date_15
        , cast(null as date) as procedure_date_16
        , cast(null as date) as procedure_date_17
        , cast(null as date) as procedure_date_18
        , cast(null as date) as procedure_date_19
        , cast(null as date) as procedure_date_20
        , cast(null as date) as procedure_date_21
        , cast(null as date) as procedure_date_22
        , cast(null as date) as procedure_date_23
        , cast(null as date) as procedure_date_24
        , cast(null as date) as procedure_date_25

    {%- endif %}

),

medical_claim_with_row_hash as (

    select *
         , {{ dbt_utils.surrogate_key([
                 'claim_id', 'claim_line_number', 'patient_id'
               , 'claim_start_date', 'claim_end_date', 'claim_line_start_date'
               , 'claim_line_end_date', 'admission_date', 'discharge_date'
               , 'claim_type', 'bill_type_code', 'place_of_service_code'
               , 'admit_source_code', 'admit_type_code'
               , 'discharge_disposition_code', 'ms_drg', 'revenue_center_code'
               , 'hcpcs_code', 'hcpcs_modifier_1', 'hcpcs_modifier_2'
               , 'hcpcs_modifier_3', 'hcpcs_modifier_4', 'hcpcs_modifier_5'
               , 'billing_npi', 'rendering_npi', 'facility_npi'
               , 'paid_date', 'paid_amount', 'charge_amount'
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
               , 'diagnosis_poa_24', 'diagnosis_poa_25', 'diagnosis_code_type'
               , 'procedure_code_type', 'procedure_code_1', 'procedure_code_2'
               , 'procedure_code_3', 'procedure_code_4', 'procedure_code_5'
               , 'procedure_code_6', 'procedure_code_7', 'procedure_code_8'
               , 'procedure_code_9', 'procedure_code_10', 'procedure_code_11'
               , 'procedure_code_12', 'procedure_code_13', 'procedure_code_14'
               , 'procedure_code_15', 'procedure_code_16', 'procedure_code_17'
               , 'procedure_code_18', 'procedure_code_19', 'procedure_code_20'
               , 'procedure_code_21', 'procedure_code_22', 'procedure_code_23'
               , 'procedure_code_24', 'procedure_code_25', 'procedure_date_1'
               , 'procedure_date_2', 'procedure_date_3', 'procedure_date_4'
               , 'procedure_date_5', 'procedure_date_6', 'procedure_date_7'
               , 'procedure_date_8', 'procedure_date_9', 'procedure_date_10'
               , 'procedure_date_11', 'procedure_date_12', 'procedure_date_13'
               , 'procedure_date_14', 'procedure_date_15', 'procedure_date_16'
               , 'procedure_date_17', 'procedure_date_18', 'procedure_date_19'
               , 'procedure_date_20', 'procedure_date_21', 'procedure_date_22'
               , 'procedure_date_23', 'procedure_date_24', 'procedure_date_25'
               ]) }}
           as row_hash
    from medical_claim_src

)

select * from medical_claim_with_row_hash
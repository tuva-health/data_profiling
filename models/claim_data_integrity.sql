{%- set table_list = ( 'eligibility', 'medical_claim' ) -%}


{%- for table_item in table_list %}

    {% set all_columns = adapter.get_columns_in_relation(
        var( table_item )
    ) %}

    {%- for column_item in all_columns %}

    select
          '{{ table_item }}' as table_name
        , '{{ column_item.name|lower }}' as column_name
        , '{{ column_item.data_type }}' as data_type

    {%- if not loop.last %}
        union all
    {% endif %}

    {%- endfor %}

    {%- if not loop.last %}
        union all
    {% endif %}

{%- endfor %}
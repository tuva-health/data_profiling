{#
    Required variable input: table_list and column_list
    This macros loops through a list of provided tables and performs
    general checks on each column.
#}

{% macro data_integrity_date_checks(table_list, column_list) %}

    {%- for table_item in table_list %}

        {%- set source_relation = adapter.get_relation(
            database = var('input_database'),
            schema = var('input_schema'),
            identifier = table_item
        ) -%}

        {%- set current_table = source_relation -%}

        {%- set all_columns = adapter.get_columns_in_relation(
            current_table
        ) -%}

        {%- for column_item in all_columns
            if column_item.name.lower() in column_list %}

            select
                  '{{ table_item }}' as table_name
                , '{{ column_item.name|lower }}' as column_name
                , (select min( {{ column_item.name }} )::varchar as min_date
                   from {{ current_table }}
                  ) as min_date
                , (select max( {{ column_item.name }} )::varchar as max_date
                   from {{ current_table }}
                  ) as max_date

            {% if not loop.last -%}
                union all
            {%- endif -%}

            {%- endfor -%}

        {% if not loop.last -%}
            union all
        {%- endif -%}

    {%- endfor -%}

{% endmacro %}
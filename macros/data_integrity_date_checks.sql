{#
    Required variable input: table_list and column_list
    This macros loops through a list of provided tables and performs
    general checks on each column.
#}

{% macro data_integrity_date_checks(table_list, column_list) %}

    {%- for table_item in table_list %}

        {%- set current_table = var( table_item ) -%}

        {%- set all_columns = adapter.get_columns_in_relation(
            current_table
        ) -%}

        {%- for column_item in all_columns
            if column_item.name.lower() in column_list %}

            select
                  '{{ table_item }}' as table_name
                , '{{ column_item.name|lower }}' as column_name
                , (select min( {{ column_item.name }} ) as min_date
                   from {{ current_table }}
                  ) as min_date
                , (select max( {{ column_item.name }} ) as max_date
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
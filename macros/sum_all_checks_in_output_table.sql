{#
    Required variable input: table_name and column_list

    This macros loops through a list of provided columns from a single table
    and sums the results of data profiling tests.
#}

{% macro sum_all_checks_in_table(table_name, column_list) %}

    {%- set source_relation = adapter.get_relation(
        database = var('output_database'),
        schema = var('output_schema'),
        identifier = table_name
    ) -%}

    {%- set current_table = source_relation -%}

    {%- set all_columns = adapter.get_columns_in_relation(
        current_table
    ) -%}

    {%- for column_item in all_columns
        if column_item.name.lower() in column_list %}

        select
              '{{ table_name }}' as table_name
            , '{{ column_item.name|lower }}' as test_name
            , sum( {{ column_item.name }} ) as test_fail_numerator
        from {{ current_table }}

        {% if not loop.last -%}
            union all
        {%- endif -%}

        {%- endfor -%}

{% endmacro %}
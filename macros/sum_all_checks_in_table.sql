{#
    Required variable input: relation and column_list

    This macros loops through a list of provided columns from a single table
    and sums the results of data profiling tests.

#}


{% macro sum_all_checks_in_table(relation, column_list) %}


    {%- set all_columns = adapter.get_columns_in_relation(
        relation
    ) -%}

    {%- for column_item in all_columns
        if column_item.name.lower() in column_list %}

        select
              '{{ relation.identifier }}' as table_name
            , '{{ column_item.name|lower }}' as test_name
            , coalesce(sum( {{ column_item.name }} ),0) as test_fail_numerator
        from {{ relation }}

        {% if not loop.last -%}
            union all
        {%- endif -%}

        {%- endfor -%}

{% endmacro %}
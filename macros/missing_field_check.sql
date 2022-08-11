{#
    This macro takes in a column to check if that column is null or blank ('').
#}

{% macro missing_field_check(column_name) %}

case
  when {{ column_name }} is null or {{ column_name }}::varchar = '' then 1
  else 0
end

{% endmacro %}
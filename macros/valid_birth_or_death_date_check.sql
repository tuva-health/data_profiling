{#
    This macro takes in a birth or death date to check if it is valid by
    comparing it to a realistic date range.
#}

{% macro valid_birth_or_death_date_check(column_name) %}

case
  when {{ column_name }} is null then 0
  when {{ column_name }} is not null
    and {{ column_name }} between '1900-01-01' and {{ current_date_or_timestamp('date') }}
    then 0
  else 1
end

{% endmacro %}
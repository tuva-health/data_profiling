{#
    This macro takes in a date from claims to check if it is valid by
    comparing it to a realistic date range.
#}

{% macro valid_claim_date_check(column_name) %}

case
  when {{ column_name }} is null then 0
  when {{ column_name }} is not null
    and {{ column_name }} between '2000-01-01' and {{ current_date_or_timestamp('date') }}
    then 0
  else 1
end

{% endmacro %}
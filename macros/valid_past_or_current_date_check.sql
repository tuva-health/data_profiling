{#
    This macro takes in a column to check if that column is a valid date by
    comparing it to a date range. This check is for past or current dates only.
    The first iteration of this macro is a very basic check.
#}

{% macro valid_past_or_current_date_check(column_name) %}

case
  when {{ column_name }} is null then 0
  when {{ column_name }} is not null
    and {{ column_name }} between '1900-01-01' and getdate()::date
    then 0
  else 1
end

{% endmacro %}
{#
    This macro takes in an enrollment date from eligibility to check if it is
    valid by comparing it to a realistic date range.
#}

{% macro valid_enrollment_date_check(column_name) %}

{%- if column_name == 'eligibility.enrollment_start_date' -%}

case
  when {{ column_name }} is null then 0
  when {{ column_name }} between '2000-01-01' and {{ current_date_or_timestamp('date') }}
    then 0
  else 1
end

{%- endif -%}

{%- if column_name == 'eligibility.enrollment_end_date' -%}

case
  when {{ column_name }} is null then 0
  when {{ column_name }} > '2000-01-01'
    then 0
  else 1
end

{%- endif -%}

{% endmacro %}
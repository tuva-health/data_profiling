{#
    This macro takes in a date from claims to check if it is valid by
    comparing it to a realistic date range.
#}

{%- macro valid_claim_date_check(column_name) -%}

    {{ return(adapter.dispatch('valid_claim_date_check')(column_name)) }}

{% endmacro %}

{%- macro bigquery__valid_claim_date_check(column_name) -%}

    case
      when {{ column_name }} is null then 0
      when safe_cast({{ column_name }} as date) is not null
        and safe_cast({{ column_name }} as date) between '2000-01-01' and {{ current_date_or_timestamp('date') }}
        then 0
      else 1
    end

{%- endmacro -%}

{%- macro default__valid_claim_date_check(column_name) -%}

    case
      when {{ column_name }} is null then 0
      when try_cast({{ column_name }} as date) is not null
        and try_cast({{ column_name }} as date) between '2000-01-01' and {{ current_date_or_timestamp('date') }}
        then 0
      else 1
    end

{%- endmacro -%}

{%- macro snowflake__valid_claim_date_check(column_name) -%}

    case
      when {{ column_name }} is null then 0
      when try_cast({{ column_name }} as date) is not null
        and try_cast({{ column_name }} as date) between '2000-01-01' and {{ current_date_or_timestamp('date') }}
        then 0
      else 1
    end

{%- endmacro -%}
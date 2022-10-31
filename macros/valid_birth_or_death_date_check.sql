{#
    This macro takes in a birth or death date to check if it is valid by
    comparing it to a realistic date range.
#}

{%- macro valid_birth_or_death_date_check(column_name) -%}

    {{ return(adapter.dispatch('valid_birth_or_death_date_check')(column_name)) }}

{%- endmacro -%}

{%- macro bigquery__valid_birth_or_death_date_check(column_name) -%}

    case
      when {{ column_name }} is null then 0
      when safe_cast({{ column_name }} as date) is not null
        and safe_cast({{ column_name }} as date) between '1900-01-01' and {{ current_date_or_timestamp('date') }}
        then 0
      else 1
    end

{%- endmacro -%}

{%- macro default__valid_birth_or_death_date_check(column_name) -%}

    case
      when {{ column_name }} is null then 0
      when try_cast({{ column_name }} as date) is not null
        and try_cast({{ column_name }} as date) between '1900-01-01' and {{ current_date_or_timestamp('date') }}
        then 0
      else 1
    end

{%- endmacro -%}

{%- macro redshift__valid_birth_or_death_date_check(column_name) -%}

    case
      when {{ column_name }} is null then 0
      when {{ column_name }} similar to '\\d{4}-\\d{2}-\\d{2}'
        and cast({{ column_name }} as date) between '1900-01-01' and {{ current_date_or_timestamp('date') }}
        then 0
      else 1
    end

{%- endmacro -%}

{%- macro snowflake__valid_birth_or_death_date_check(column_name) -%}

    case
      when {{ column_name }} is null then 0
      when try_cast({{ column_name }} as date) is not null
        and try_cast({{ column_name }} as date) between '1900-01-01' and {{ current_date_or_timestamp('date') }}
        then 0
      else 1
    end

{%- endmacro -%}
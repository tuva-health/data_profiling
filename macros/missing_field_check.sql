{#
    This macro takes in a column to check if that column is null or blank ('').
#}

{%- macro missing_field_check(column_name) -%}

    {{ return(adapter.dispatch('missing_field_check')(column_name)) }}

{%- endmacro -%}

{%- macro bigquery__missing_field_check(column_name) -%}

    case
      when {{ column_name }} is null or cast({{ column_name }} as string) = '' then 1
      else 0
    end

{%- endmacro -%}

{%- macro default__missing_field_check(column_name) %}

    case
      when {{ column_name }} is null or cast({{ column_name }} as string) = '' then 1
      else 0
    end

{%- endmacro -%}

{%- macro redshift__missing_field_check(column_name) -%}

    case
      when {{ column_name }} is null or cast({{ column_name }} as varchar) = '' then 1
      else 0
    end

{%- endmacro -%}

{%- macro snowflake__missing_field_check(column_name) %}

    case
      when {{ column_name }} is null or cast({{ column_name }} as string) = '' then 1
      else 0
    end

{%- endmacro -%}
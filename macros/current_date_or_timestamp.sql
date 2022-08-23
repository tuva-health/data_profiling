{#-
    Returns current date or timestamp in supplied date format (defaults to date)
    depending on adapter.
-#}

{%- macro current_date_or_timestamp(date_format='date') -%}

    {{ return(adapter.dispatch('current_date_or_timestamp')(date_format)) }}

{%- endmacro -%}

{%- macro bigquery__current_date_or_timestamp(date_format) -%}

    cast(current_timestamp as {{date_format }})

{%- endmacro -%}

{%- macro default__current_date_or_timestamp(date_format) %}

    cast(current_timestamp() as {{date_format }})

{%- endmacro -%}

{%- macro redshift__current_date_or_timestamp(date_format) -%}

    cast(getdate() as {{date_format }})

{%- endmacro -%}

{%- macro snowflake__current_date_or_timestamp(date_format) %}

    cast(current_timestamp() as {{date_format }})

{%- endmacro -%}
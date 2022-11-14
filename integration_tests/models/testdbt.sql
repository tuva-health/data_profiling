
{% set relation_exists = (load_relation(source('claims_input','eligibility'))) is not none %} {#i hate dbt, this doesnt work as a ref like they have it in their documentation #}
{% if relation_exists %}
      {{ log("my_model has already been built", info=true) }}
{% else %}
      {{ log("my_model doesn't exist in the warehouse. Maybe it was dropped?", info=true) }}
{% endif %}

    select 1 as a
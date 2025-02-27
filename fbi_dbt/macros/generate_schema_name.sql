{% macro generate_schema_name(custom_schema_name, node) %}
  {% if target.name == 'prod' %}
    {{ custom_schema_name }}
  {% else %}
    dev_{{ custom_schema_name }}
  {% endif %}
{% endmacro %}

{% macro trim_and_stdize_nulls(column_name, pattern='[\s\n]') %}
    nullif(regexp_replace({{ column_name }}, '{{ pattern }}', '', 'g'), '')
{% endmacro %}
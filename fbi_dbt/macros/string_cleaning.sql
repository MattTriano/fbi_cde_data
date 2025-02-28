{% macro trim_and_stdize_nulls(column_name) %}
    nullif(regexp_replace(trim({{ column_name }}), '[\s\n#]', '', 'g'), '')
{% endmacro %}
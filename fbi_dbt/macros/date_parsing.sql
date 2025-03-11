{% macro parse_nibrs_date(column_name, min_year = '1991') %}
    case
        when length(trim({{ column_name }})) = 8
             and {{ column_name }} ~ '^[0-9]{8}$'
             and substr({{ column_name }}, 1, 4) BETWEEN '{{ min_year }}' AND '2039'
        then try_cast(strptime(trim({{ column_name }}), '%Y%m%d') as date)
        else NULL
    end
{% endmacro %}

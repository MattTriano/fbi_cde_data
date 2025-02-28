{% macro parse_nibrs_date(column_name) %}
    case
        when length(trim({{ column_name }})) = 8
             and {{ column_name }} ~ '^[0-9]{8}$'
             and substr({{ column_name }}, 1, 4) BETWEEN '1991' AND '2039'
        then try_cast(strptime(trim({{ column_name }}), '%Y%m%d') as date)
        else NULL
    end
{% endmacro %}

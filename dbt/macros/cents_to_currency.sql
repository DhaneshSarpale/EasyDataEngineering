{# ================================================================
   Reusable macros.
   ================================================================ #}

{# Convert an integer-cents column to a decimal currency amount. #}
{% macro cents_to_currency(column_name) %}
    round(cast({{ column_name }} as decimal(18,2)) / 100.0, 2)
{% endmacro %}


{# Standardised amount band used across marts (matches advanced_sql.sql). #}
{% macro amount_band(column_name) %}
    case
        when {{ column_name }} >= 5000 then 'HIGH'
        when {{ column_name }} >= 500  then 'MEDIUM'
        else 'LOW'
    end
{% endmacro %}

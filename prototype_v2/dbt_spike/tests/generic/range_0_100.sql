{% test range_0_100(model, column_name) %}
SELECT * FROM {{ model }} WHERE {{ column_name }} NOT BETWEEN 0 AND 100
{% endtest %}

{% test latitude_check(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} < -90 OR {{ column_name }} > 90

{% endtest %}

{% test longitude_check(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} < -180 OR {{ column_name }} > 180

{% endtest %}

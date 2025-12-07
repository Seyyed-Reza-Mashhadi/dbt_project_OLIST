-- Singular test to validate that latitude and longitude values are within valid ranges
{{ config(store_failures = true) }}

SELECT latitude, longitude
FROM {{ ref('STG_geolocation') }}
WHERE latitude < -90 OR latitude > 90 OR longitude < -180 OR longitude > 180
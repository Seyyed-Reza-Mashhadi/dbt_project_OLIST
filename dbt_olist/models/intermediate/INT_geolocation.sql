-- This intermediate model calculates the average latitude and longitude for each zip code
-- Based on the location of Brazil, longitude and latitude should be within the following ranges
-- there are multiple entries with invalid values that need to be excluded 
-- otherwise BI tools will have issues plotting the data on a map

SELECT
    zip_code_prefix,
    ANY_VALUE(city) AS city,
    ANY_VALUE(province) AS province,
    AVG(latitude) AS latitude,
    AVG(longitude) AS longitude
FROM {{ ref('STG_geolocation') }}
WHERE (latitude > -30 AND latitude < 10) 
    AND (longitude > -80 AND longitude < -30)
GROUP BY zip_code_prefix
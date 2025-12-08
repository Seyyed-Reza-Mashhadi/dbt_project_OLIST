-- DIM_sellers.sql
SELECT
    s.seller_id,
    s.zip_code_prefix,
    s.city,
    s.province,
    b.name as province_name,
    sg.longitude,
    sg.latitude
FROM {{ ref('STG_sellers') }} AS s
LEFT JOIN {{ ref('INT_geolocation') }} AS sg
    ON sg.zip_code_prefix = s.zip_code_prefix
LEFT JOIN {{ ref("brazil_states") }} AS b
    ON b.abbreviation = sg.province

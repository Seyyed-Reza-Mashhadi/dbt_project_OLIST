-- DIM_customers.sql
SELECT  
    ----- columns from STG_customers
    sc.customer_id,
    sc.customer_unique_id,
    sc.zip_code_prefix,
    sc.city,
    sc.province,
    -- full state name from brazil_states.csv 
    b.name as province_name,
    -- geolocation columns will be added later
    sg.latitude,
    sg.longitude

From {{ ref('STG_customers') }} as sc
LEFT JOIN {{ ref('INT_geolocation') }} AS sg
    ON sg.zip_code_prefix = sc.zip_code_prefix
LEFT JOIN {{ ref("brazil_states") }} AS b
    ON b.abbreviation = sg.province
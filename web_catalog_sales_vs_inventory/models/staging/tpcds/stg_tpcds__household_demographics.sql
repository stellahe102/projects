-- rename the source columns
SELECT
    HD_BUY_POTENTIAL AS BUY_POTENTIAL, 
    HD_INCOME_BAND_SK AS INCOME_BAND_SK, 
    HD_DEMO_SK AS DEMO_SK, 
    HD_DEP_COUNT AS DEP_COUNT, 
    HD_VEHICLE_COUNT AS VEHICLE_COUNT
FROM 
    {{ source('tpcds', 'household_demographics') }}
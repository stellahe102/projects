-- rename the source columns
SELECT 
    D_DATE_ID AS DATE_ID, 
    CAL_DT, 
    DAY_OF_WK_DESC, 
    WK_NUM, 
    MNTH_NUM, 
    YR_MNTH_NUM, 
    D_DATE_SK, 
    YR_NUM, 
    DAY_OF_WK_NUM, 
    YR_WK_NUM, 
    _AIRBYTE_NORMALIZED_AT
FROM {{ source('tpcds', 'date_dim')}}
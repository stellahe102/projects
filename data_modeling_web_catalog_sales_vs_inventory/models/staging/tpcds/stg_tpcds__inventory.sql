-- rename columns by removing the prefix
SELECT 
    INV_DATE_SK AS DATE_SK,
    INV_ITEM_SK AS ITEM_SK,
    INV_QUANTITY_ON_HAND AS QUANTITY_ON_HAND,
    INV_WAREHOUSE_SK AS WAREHOUSE_SK
FROM
    {{ source('tpcds', 'inventory') }}
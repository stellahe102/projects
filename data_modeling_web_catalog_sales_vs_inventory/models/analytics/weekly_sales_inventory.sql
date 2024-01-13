
{{
    config(
        materialized='incremental',
        target_schema='analytics',
        unique_key=['warehouse_sk', 'item_sk', 'sold_date_bow_sk']
    )
}}

WITH 
    -- aggregate sales data to weekly level
    agg_wkly_sales AS 
    (
        SELECT 
            warehouse_sk, 
            item_sk, 
            wk_num, 
            yr_num,
            sold_date_bow_sk,
            sold_date_bow_cal_date,
            SUM(daily_quantity) AS  weekly_quantity, 
            SUM(daily_net_profit) AS weekly_net_profit, 
            SUM(daily_sales_amt) AS weekly_sales_amt,
            SUM(daily_quantity)/7 AS avg_qty_dy
        FROM {{ ref('int__daily_aggregated_sales') }}

        {% if is_incremental() %} 

        WHERE 
        sold_date_bow_sk >= NVL((select MAX(sold_date_bow_sk) from {{ this }}), 0)

        {% endif %}        
        
        GROUP BY 1, 2, 3, 4, 5, 6),
    -- aggregate inventory data to weekly level
    -- step 1.
    -- each warehouse and item's last day of recording to inventory table is different
    -- find the last day of each week (eow) to for each item+warehouse to pull the quantity in next step
    eow_inventory_day AS
    (
        SELECT 
            WAREHOUSE_SK,
            ITEM_SK,
            YR_num,
            WK_num,
            MAX(DATE_SK) AS eow_sk
        FROM {{ ref('stg_tpcds__inventory') }}
        INNER JOIN {{ ref('stg_tpcds__date_dim') }}
            ON date_sk = d_date_sk
        GROUP BY 1, 2, 3, 4
        ORDER BY 1, 2, 3, 4),

        
    -- aggregate inventory data to weekly level
    -- step 2.            
    -- using the last day (from inv table) of the week to find weekly qty on hand     
    agg_wkly_inventory_stg AS
    (
        SELECT DISTINCT
            inv.WAREHOUSE_SK AS WAREHOUSE_SK,
            inv.ITEM_SK AS ITEM_SK,
            eow.YR_num AS YR_num,
            eow.WK_num AS WK_num,
            case 
                when inv.date_sk = eow.eow_sk 
                    THEN inv.QUANTITY_ON_HAND
                else NULL
                end as inv_on_hand_qty_wk
        FROM {{ ref('stg_tpcds__inventory') }} AS inv
        INNER JOIN eow_inventory_day AS eow USING(WAREHOUSE_SK, ITEM_SK)
        HAVING inv_on_hand_qty_wk is not null
    ),

    -- final weekly table, join inv and sales
    agg_wkly_inventory AS
    (
        select 
            sales.WAREHOUSE_SK AS WAREHOUSE_SK,
            sales.ITEM_SK AS ITEM_SK,
            sales.sold_date_bow_sk AS sold_date_bow_sk,
            sales.yr_num AS SOLD_YR_NUM,
            sales.wk_num AS SOLD_WK_NUM,
            sales.weekly_quantity AS SUM_QTY_WK,
            sales.weekly_sales_amt AS SUM_AMT_WK,
            sales.weekly_net_profit AS SUM_PROFIT_WK,
            sales.avg_qty_dy AS AVG_QTY_DY,
            inv.inv_on_hand_qty_wk AS inv_on_hand_qty_wk,
            inv.inv_on_hand_qty_wk/sales.weekly_quantity AS wks_sply,
            case 
                when sales.avg_qty_dy > 0 and sales.avg_qty_dy > inv.inv_on_hand_qty_wk
                then True
                else false
                end as low_stock_flg_wk
        FROM agg_wkly_sales AS sales 
        INNER JOIN agg_wkly_inventory_stg AS inv
            ON inv.WAREHOUSE_SK = sales.warehouse_sk
                AND inv.item_sk = sales.item_sk
                AND inv.wk_num = sales.wk_num
                AND inv.yr_num = sales.yr_num
    )

SELECT DISTINCT
    WAREHOUSE_SK,
    ITEM_SK,
    sold_date_bow_sk,
    SOLD_YR_NUM,
    SOLD_WK_NUM,
    SUM_QTY_WK,
    SUM_AMT_WK,
    SUM_PROFIT_WK,
    AVG_QTY_DY,
    inv_on_hand_qty_wk,
    wks_sply,
    low_stock_flg_wk
FROM agg_wkly_inventory

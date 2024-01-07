{{
    config(
        materialized='incremental',
        target_schema='intermediate',
        unique_key=['warehouse_sk', 'item_sk', 'sold_date_sk']
    )
}}
WITH 
    -- union the web sales and catalog sales data into one view
    cs_ws_union AS
        (
        /* assumes order_number is the atomic level, extract order_number to make sure all transactions are pulled,
            so that even 2 similar transactions (same item, same warehouse, same date and quantity, different order no.)
            happen, both rows of data are extracted.
            use distinct to avoid duplicate */
        SELECT DISTINCT
            ORDER_NUMBER AS order_number,
            ITEM_SK AS item_sk,
            WAREHOUSE_SK AS warehouse_sk,
            SOLD_DATE_SK as sold_date_sk,
            QUANTITY as quantity,
            NET_PROFIT as net_profit,
            SALES_PRICE as sales_price,
            SALES_PRICE * QUANTITY AS sales_amt
        FROM {{ ref('stg_tpcds__web_sales') }}
        WHERE 
            quantity IS NOT NULL
            AND sales_amt IS NOT NULL
            AND WAREHOUSE_SK IS NOT NULL
        UNION
        SELECT DISTINCT
            ORDER_NUMBER AS order_number,
            ITEM_SK AS item_sk,
            WAREHOUSE_SK AS warehouse_sk,
            SOLD_DATE_SK as sold_date_sk,
            QUANTITY as quantity,
            NET_PROFIT as net_profit,
            SALES_PRICE as sales_price,
            sales_price * quantity AS sales_amt
        FROM {{ ref('stg_tpcds__catalog_sales') }}
        WHERE 
            quantity IS NOT NULL
            AND sales_amt IS NOT NULL
            AND WAREHOUSE_SK IS NOT NULL
        ),
    -- aggregate all the sales data to daily level
    daily_agg AS
        (
            select 
                item_sk,
                warehouse_sk,
                sold_date_sk,
                sum(quantity) AS daily_quantity,
                SUM(net_profit) AS daily_net_profit,
                SUM(sales_amt) AS daily_sales_amt
            From cs_ws_union
            {% if is_incremental() %} 

            WHERE 
            sold_date_sk >= NVL((select MAX(sold_date_sk) from {{ this }}), 0)

            {% endif %}
            GROUP BY 1, 2, 3
        ),
    -- get the date surrogate key for the first day/begin of week (bow) 
    bow as 
        (
            select 
                wk_num, 
                yr_num, 
                d_date_sk as bow_sk,
                cal_dt as cal_date
            from {{ ref('stg_tpcds__date_dim') }}
            where day_of_wk_num = 0)
    -- add the bow sk to the daily table
SELECT 
    item_sk, 
    warehouse_sk, 
    sold_date_sk, 
    {{ dbt_utils.generate_surrogate_key(['item_sk', 'warehouse_sk']) }} as item_warehouse_sk,
    dt.cal_dt as sold_cal_date,
    daily_quantity, 
    daily_net_profit, 
    daily_sales_amt,
    dt.wk_num as wk_num, 
    dt.yr_num as yr_num,
    bow.bow_sk AS sold_date_bow_sk,
    bow.cal_date as sold_date_bow_cal_date
FROM daily_agg
INNER JOIN {{ ref('stg_tpcds__date_dim') }} AS dt 
    ON daily_agg.sold_date_sk = dt.d_date_sk 
INNER JOIN bow 
    ON dt.wk_num = bow.wk_num AND dt.yr_num = bow.yr_num

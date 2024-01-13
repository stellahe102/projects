-- create a snapshot of the basic customer data
-- strategy is check
{%  snapshot customer_snapshot %}

{{
    config(
        target_database='tpcds',
        target_schema='intermediate',
        strategy='check',
        unique_key='CUSTOMER_SK',
        check_cols=['SALUTATION',
            'PREFERRED_CUST_FLAG',
            'FIRST_SALES_DATE_SK',
            'LOGIN',
            'CURRENT_CDEMO_SK',
            'FIRST_NAME',
            'CURRENT_HDEMO_SK',
            'CURRENT_ADDR_SK',
            'LAST_NAME',
            'CUSTOMER_ID',
            'LAST_REVIEW_DATE_SK',
            'BIRTH_MONTH',
            'BIRTH_COUNTRY',
            'BIRTH_YEAR',
            'BIRTH_DAY',
            'EMAIL_ADDRESS',
            'FIRST_SHIPTO_DATE_SK'] 
    )
}}

SELECT DISTINCT
    SALUTATION,
    PREFERRED_CUST_FLAG,
    FIRST_SALES_DATE_SK,
    CUSTOMER_SK,
    LOGIN,
    CURRENT_CDEMO_SK,
    FIRST_NAME,
    CURRENT_HDEMO_SK,
    CURRENT_ADDR_SK,
    LAST_NAME,
    CUSTOMER_ID,
    LAST_REVIEW_DATE_SK,
    BIRTH_MONTH,
    BIRTH_COUNTRY,
    BIRTH_YEAR,
    BIRTH_DAY,
    EMAIL_ADDRESS,
    FIRST_SHIPTO_DATE_SK,
    _AIRBYTE_NORMALIZED_AT 
FROM
    {{ ref('stg_tpcds__customer') }}

{% endsnapshot %}
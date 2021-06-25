{{config(
    materialized = 'incremental',
    unique_key = 'id',
    partition_by = { 'field': 'date', 'data_type': 'timestamp' },
    incremental_strategy = 'insert_overwrite'
)}}


with leads as ( select * from {{ ref('stg_leads') }}),
transactions as ( select * from {{ ref('stg_transactions') }})

select
    l.*,
    CASE 
        WHEN EXISTS (SELECT 1 FROM transactions t WHERE t.Lead_Id = l.id) and l.ProjectStatus != "Free" THEN 1 
        ELSE 0 
    END as Billable
from leads l


{% if is_incremental() %}

where date(Date) > (select max(date(Date)) from {{ this }})

-- where date(date) >= date_sub(_dbt_max_partition, interval 1 day)

{% endif %}
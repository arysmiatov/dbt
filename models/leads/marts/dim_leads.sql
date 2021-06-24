{{ config(
    materialized='incremental',
    unique_key='id'
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

  -- this filter will only be applied on an incremental run
  where l.Date > (select max(l.Date) from {{ this }})

{% endif %}

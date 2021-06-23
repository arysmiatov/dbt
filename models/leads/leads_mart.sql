{{ config ( 
    materialized="table" 
)}}

with leads as ( select * from {{ ref('stg_leads') }}),
transactions as ( select * from {{ ref('stg_transactions') }})

select
    l.*,
    CAST(CASE WHEN EXISTS (SELECT 1 FROM transactions t WHERE t.Lead_Id = l.id) and l.ProjectStatus != 2 THEN 1 ELSE 0 END as bit) as Billable
from leads l


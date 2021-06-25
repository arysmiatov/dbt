{{config(
    materialized = 'table'
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
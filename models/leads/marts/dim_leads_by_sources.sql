{{ config ( 
    materialized="table" 
)}}

with leads as ( select * from {{ ref('stg_leads') }})


select
    l.date,
    l.leadSource,
    count(*) as NumberOfLeads
from leads l
group by l.date, l.leadSource

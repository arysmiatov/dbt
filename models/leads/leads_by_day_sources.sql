{{ config ( 
    materialized="table"  -- Если тут пусто, то вместо таблицы будет создано View
)}}

with leads as {{ ref=('stg_leads') }}

select
    l.date
    l.leadSource,
    count(*) as NumberOfLeads
from leads l
group 1,2

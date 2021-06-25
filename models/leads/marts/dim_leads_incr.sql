{% set partitions_to_replace = [
    'current_date',
    'date_sub(current_date, interval 2 day)'
] %}

{{config(
    materialized = 'incremental',
    partition_by = { 'field': 'date', 'data_type': 'timestamp' },
    incremental_strategy = 'insert_overwrite',
    partitions = partitions_to_replace
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

where date(date) in ({{ partitions_to_replace | join(',') }})

{% endif %}
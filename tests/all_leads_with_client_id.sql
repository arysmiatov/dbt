select
    l.id
from {{ ref('stg_leads') }} l
where 
    l.clientid is null
    and cast(date as date) > '2021-06-01'
{{ config ( 
    materialized="view" 
)}}

with transactions as (
    select
        tr.Amount, 
        tr.SaleId, 
        tr.DealId,
        tr.Lead_Id
    from `homsters-kz-dwh.raw_data.transactions` tr
)

select * from transactions 
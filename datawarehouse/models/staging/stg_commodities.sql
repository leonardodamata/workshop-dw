-- import
with source as (
  select 
    "True",
    "Close",
    simbolo
  from 
    {{ source ('dbsalesaovivo_wx6c','commodities')}}
),
-- renamed

renamed as (

  select
    cast("True" as date) as data,
    "Close" as valor_fechamento,
    simbolo
  from
    source  
)

-- select * from 

select * from renamed
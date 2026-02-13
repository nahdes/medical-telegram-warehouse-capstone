
    
    

with all_values as (

    select
        channel_type as value_field,
        count(*) as n_records

    from "medical_warehouse"."public_staging"."stg_telegram_messages"
    group by channel_type

)

select *
from all_values
where value_field not in (
    'Pharmaceutical','Cosmetics','Medical','Other'
)




    
    

with all_values as (

    select
        has_media as value_field,
        count(*) as n_records

    from "medical_warehouse"."public_staging"."stg_telegram_messages"
    group by has_media

)

select *
from all_values
where value_field not in (
    'True','False'
)



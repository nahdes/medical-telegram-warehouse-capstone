
    
    

with all_values as (

    select
        is_empty_message as value_field,
        count(*) as n_records

    from "medical_warehouse"."public_staging"."stg_telegram_messages"
    group by is_empty_message

)

select *
from all_values
where value_field not in (
    'True','False'
)



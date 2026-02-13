
    
    

with all_values as (

    select
        mentions_availability as value_field,
        count(*) as n_records

    from "medical_warehouse"."public"."fct_messages"
    group by mentions_availability

)

select *
from all_values
where value_field not in (
    'True','False'
)



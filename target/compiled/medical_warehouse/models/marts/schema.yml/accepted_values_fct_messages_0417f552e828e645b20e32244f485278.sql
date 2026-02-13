
    
    

with all_values as (

    select
        content_type as value_field,
        count(*) as n_records

    from "medical_warehouse"."public"."fct_messages"
    group by content_type

)

select *
from all_values
where value_field not in (
    'Image Only','Image with Text','Text Only','Empty'
)



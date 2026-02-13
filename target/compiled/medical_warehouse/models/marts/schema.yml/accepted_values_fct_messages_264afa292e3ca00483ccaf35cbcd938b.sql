
    
    

with all_values as (

    select
        engagement_category as value_field,
        count(*) as n_records

    from "medical_warehouse"."public"."fct_messages"
    group by engagement_category

)

select *
from all_values
where value_field not in (
    'No Views','Low Engagement','Medium Engagement','High Engagement'
)



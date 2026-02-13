
    
    

with all_values as (

    select
        is_weekday as value_field,
        count(*) as n_records

    from "medical_warehouse"."public_marts"."dim_dates"
    group by is_weekday

)

select *
from all_values
where value_field not in (
    'True','False'
)



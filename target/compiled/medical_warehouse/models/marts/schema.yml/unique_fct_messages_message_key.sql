
    
    

select
    message_key as unique_field,
    count(*) as n_records

from "medical_warehouse"."public"."fct_messages"
where message_key is not null
group by message_key
having count(*) > 1




    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from "medical_warehouse"."public_test_failures"."accepted_values_fct_messages_0417f552e828e645b20e32244f485278"
    
      
    ) dbt_internal_test

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from "medical_warehouse"."public_test_failures"."accepted_values_fct_messages_has_image__True__False"
    
      
    ) dbt_internal_test
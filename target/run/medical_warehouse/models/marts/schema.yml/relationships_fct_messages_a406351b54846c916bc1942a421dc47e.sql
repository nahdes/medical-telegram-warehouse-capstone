
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from "medical_warehouse"."public_test_failures"."relationships_fct_messages_a406351b54846c916bc1942a421dc47e"
    
      
    ) dbt_internal_test
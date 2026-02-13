
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from "medical_warehouse"."public_test_failures"."accepted_values_dim_channels_56eee5ed7d44ea4b442512e8cafb305f"
    
      
    ) dbt_internal_test
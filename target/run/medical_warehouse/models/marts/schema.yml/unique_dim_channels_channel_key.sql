
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from "medical_warehouse"."public_test_failures"."unique_dim_channels_channel_key"
    
      
    ) dbt_internal_test
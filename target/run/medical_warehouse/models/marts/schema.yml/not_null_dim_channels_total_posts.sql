
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from "medical_warehouse"."public_test_failures"."not_null_dim_channels_total_posts"
    
      
    ) dbt_internal_test
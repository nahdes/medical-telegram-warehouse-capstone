
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from "medical_warehouse"."public_test_failures"."relationships_fct_messages_date_key__date_key__ref_dim_dates_"
    
      
    ) dbt_internal_test
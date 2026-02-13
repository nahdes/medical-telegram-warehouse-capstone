
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from "medical_warehouse"."public_test_failures"."accepted_values_stg_telegram_m_1fe57487cf22efaa04a3dccf15525ec0"
    
      
    ) dbt_internal_test
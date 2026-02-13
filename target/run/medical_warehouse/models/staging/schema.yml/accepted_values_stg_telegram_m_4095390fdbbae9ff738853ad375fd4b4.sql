
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from "medical_warehouse"."public_test_failures"."accepted_values_stg_telegram_m_4095390fdbbae9ff738853ad375fd4b4"
    
      
    ) dbt_internal_test
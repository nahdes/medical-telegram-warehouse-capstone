
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from "medical_warehouse"."public_test_failures"."not_null_stg_telegram_messages_message_id"
    
      
    ) dbt_internal_test
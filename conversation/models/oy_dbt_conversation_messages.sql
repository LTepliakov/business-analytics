{{ config(materialized='table', sort=['Account_Name', 'Creation_Date']) }}

select * from {{ source('analytics', 'oy_conversation_messages_2023_H1') }}
union
select * from {{ ref('oy_dbt_conversation_messages_append') }}
/*
Model: chw_activity_monthly
Description: Monthly aggregation of CHW activities for dashboard performance metrics

TODO: Complete this dbt model to aggregate CHW activities by month

Instructions:
1. Add the dbt config block (materialization, unique_key, incremental_strategy)
2. Filter out invalid records (NULL chv_id, NULL activity_date, deleted records)
3. Use the month_assignment macro to calculate report_month
4. Aggregate metrics: total_activities, unique_households_visited, unique_patients_served, pregnancy_visits, child_assessments, family_planning_visits
5. GROUP BY chv_id and report_month
6. Add incremental logic 
*/

-- ============================================
-- TODO: Add dbt config block here
-- Required: materialized, unique_key, incremental_strategy
-- See business_requirements.md for materialization requirements
-- ============================================


-- ============================================
-- Main Query
-- ============================================

with source_data as (

    select
        activity_id,
        chv_id,
        activity_date,
        activity_type,
        household_id,
        patient_id,
        is_deleted,
        created_at,
        updated_at
    from {{ ref('fct_chv_activity') }}

    where 1=1


),

with_report_month as (

    select
        *,
        NULL as report_month

    from source_data

),

aggregated as (

    select
        chv_id,
        report_month

    from with_report_month


)

select * from aggregated


{{
    config(
        materialized='incremental',
        unique_key=['chv_id', 'report_month'],
        incremental_strategy='delete+insert'
    )
}}


with source_data as (   
    select
        activity_id,
        chv_id,
        activity_date,
        activity_type,
        household_id,
        patient_id,
        is_deleted,
        created_at,
        updated_at
    from {{ ref('fct_chv_activity') }}
    
    where 1=1
        and chv_id IS NOT NULL           
        and activity_date IS NOT NULL    
        and is_deleted = FALSE           
        
    {% if is_incremental() %}
        and activity_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '2 months'
    {% endif %}
),

with_report_month as (
    select
        *,
        {{ month_assignment('activity_date') }} as report_month
    from source_data
),

aggregated as (
    select
        chv_id,
        report_month,
        
        COUNT(*) as total_activities,
        
        COUNT(DISTINCT household_id) as unique_households_visited,
        COUNT(DISTINCT patient_id) as unique_patients_served,
        -- NB: COUNT(DISTINCT ...) automatically ignores NULLs
        
        COUNT(CASE WHEN activity_type = 'pregnancy_visit' THEN 1 END) as pregnancy_visits,
        COUNT(CASE WHEN activity_type = 'child_assessment' THEN 1 END) as child_assessments,
        COUNT(CASE WHEN activity_type = 'family_planning' THEN 1 END) as family_planning_visits
        

        
    from with_report_month
    
    group by 
        chv_id,
        report_month
)

select * from aggregated
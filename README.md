# CAPSTONE-PROJECT
Monthly CHW Activity Aggregation Model

# CHW Monthly Activity Aggregation Project

**JOAN WAVINYA**
 

## Project Overview
This dbt project transforms Community Health Worker (CHW) activity data into monthly performance metrics for healthcare analytics dashboards in Kenya.

## Problem:
Healthcare analytics teams needed a performant way to track CHW productivity across regions. The source data contains individual visit records, but dashboards require monthly aggregations with special business rules for delayed field reporting.

## Solution: 

### 1. Month Assignment Macro (`macros/month_assignment.sql`)
**Purpose:** Implements the 26th-of-month cutoff rule for field reporting delays.

**Logic:**
- Activities before day 26 → Assigned to current month
- Activities on/after day 26 → Assigned to next month
- Correctly handles year boundaries (Dec 26 → Jan next year)

**Why:** Remote field workers often submit late reports. Activities on Jan 26-31 are actually "closing out" January work, so they count toward February's reporting period.

### 2. Aggregation Model (`models/metrics/chw_activity_monthly.sql`)
**Configuration:**
- **Materialization:** Incremental table (performance optimization)
- **Strategy:** delete+insert (handles late-arriving data)
- **Unique Key:** (chv_id, report_month)
- **Incremental Logic:** Reprocesses last 2 months on each run

**Metrics Calculated:**
1. `total_activities` - All activities count
2. `unique_households_visited` - Distinct households (deduplication)
3. `unique_patients_served` - Distinct patients (handles NULLs)
4. `pregnancy_visits` - Conditional aggregation
5. `child_assessments` - Conditional aggregation
6. `family_planning_visits` - Conditional aggregation

**Data Quality Filters:**
- Excludes NULL chv_id (data entry errors)
- Excludes NULL activity_date (invalid records)
- Excludes soft-deleted records (is_deleted = TRUE)

### 3. Data Quality Tests (`models/metrics/schema.yml`)
**Tests Implemented:**
- `not_null` on all key fields
- Comprehensive column documentation
- Ensures data integrity for dashboard consumption

## Technical Concepts Demonstrated

### 1. Incremental Materialization
Instead of rebuilding the entire table daily, only new/changed data is processed. This reduces query time from hours to minutes as data grows.

### 2. Reusable Macros
The month_assignment logic is encapsulated in a macro, allowing reuse across multiple models without code duplication.

### 3. CTE Pattern (Common Table Expressions)
SQL is organized into logical steps:
- `source_data` - Filtered raw data
- `with_report_month` - Month assignment applied
- `aggregated` - Final metrics calculated

### 4. Conditional Aggregation
Using `COUNT(CASE WHEN ... THEN 1 END)` pattern instead of multiple subqueries improves performance and readability.

### 5. Delete+Insert Strategy
Allows reprocessing of historical months if late data arrives, ensuring data accuracy over time.

## Expected Output 

| chv_id | report_month | total_activities | unique_households | pregnancy_visits |
|--------|--------------|------------------|-------------------|------------------|
| CHV001 | 2025-01-01   | 4                | 2                 | 3                |
| CHV001 | 2025-02-01   | 3                | 3                 | 1                |
| CHV002 | 2025-01-01   | 3                | 2                 | 0                |
| CHV002 | 2025-02-01   | 1                | 1                 | 1                |
| CHV003 | 2025-02-01   | 2                | 1                 | 0                |
| CHV006 | 2025-01-01   | 2                | 1                 | 1                |

## Business Impact
This model enables:
- Real-time CHW performance monitoring
- Identification of high/low performers
- Resource allocation decisions
- Trend analysis over time
- Regional productivity comparisons

## How to Run
```bash
# Compile and check for errors
dbt compile --select chw_activity_monthly

# Run the model
dbt run --select chw_activity_monthly

# Run data quality tests
dbt test --select chw_activity_monthly
```

## Learning Outcomes
Through this project, I demonstrated understanding of:
-  dbt project structure and configuration
-  Incremental materialization strategies
-  SQL aggregation and window functions
-  Data quality testing frameworks
-  Translating business requirements into technical solutions
-  Version control with Git/GitHub
-  Documentation best practices

## Files Submitted
1. `macros/month_assignment.sql` - Reusable business logic
2. `models/metrics/chw_activity_monthly.sql` - Main aggregation model
3. `models/metrics/schema.yml` - Tests and documentation

---

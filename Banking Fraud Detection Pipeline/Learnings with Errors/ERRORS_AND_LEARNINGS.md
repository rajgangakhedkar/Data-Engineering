# Errors Faced and What I Learned

This section documents the main errors and issues faced while building the Banking Fraud Detection Pipeline. These problems helped improve the final design of the Snowflake procedures and Airflow orchestration.

---

## 1. Ambiguous Column Name: `BATCH_ID`

### Error

```text
SQL compilation error: ambiguous column name 'BATCH_ID'
```

### Why it happened

This happened when multiple tables or CTEs had a column named `batch_id`, but the SQL query did not clearly mention which table the column should come from.

### Fix

Used table aliases and fully qualified column names such as:

```sql
f2.batch_id
cu.batch_id
lc.batch_id
```

### Learning

When using `JOIN`, `MERGE`, or nested CTEs, common column names like `batch_id`, `customer_id`, and `device_id` should always be referenced with aliases.

---

## 2. Snowflake Procedure Failing Inside Airflow

### Error

```text
Uncaught exception of type 'STATEMENT_ERROR'
```

### Why it happened

The Airflow DAG was calling Snowflake stored procedures in sequence. If one procedure failed, the complete DAG stopped at that task.

### Fix

Tested each Snowflake procedure manually before running it through Airflow. Then the DAG was used only for orchestration after confirming that each procedure worked independently.

### Learning

In an Airflow + Snowflake project, debug SQL procedures first in Snowflake, then connect them to Airflow.

---

## 3.  Processing All Data Instead of Only the Latest Batch

### Problem

Initially, transformation logic could process older raw records again instead of only the newest uploaded file.

### Fix

Added `batch_id` using `METADATA$FILENAME` during raw loading and used a `latest_batch` CTE:

```sql
WITH latest_batch AS (
    SELECT batch_id
    FROM banking_data.data_raw.transactions_raw
    QUALIFY ROW_NUMBER() OVER (ORDER BY load_time DESC) = 1
)
```

### Learning

For incremental pipelines, every file or batch should have a tracking column like `batch_id`, `load_time`, or `file_name`.

---

## 4. Duplicate Records Entering Curated Tables

### Problem

Duplicate transaction IDs or login event IDs could enter the curated layer.

### Fix

Used `ROW_NUMBER()` to detect duplicates:

```sql
ROW_NUMBER() OVER (PARTITION BY txn_id ORDER BY txn_id) AS dups
```

Valid records were loaded only when `dups = 1`, and duplicate records were stored separately in fraud tables.

### Learning

A good pipeline should not simply delete bad data. It should separate valid records and rejected records so the issue can be audited later.

---

## 5. Invalid Date and Amount Values

### Problem

Some records had invalid timestamps or non-numeric amount values.

### Fix

Used safe conversion functions:

```sql
TRY_TO_TIMESTAMP(txn_time, 'MM/DD/YYYY HH24:MI')
TRY_CAST(amount AS INT)
```

### Learning

`TRY_TO_TIMESTAMP` and `TRY_CAST` are safer than direct casting because they return `NULL` instead of failing the full pipeline.

---

## 7.  Unknown Device Logic Confusion

### Problem

There was confusion about how to detect a new device login for a customer.

### Fix

Created a known-device reference table and inserted the first device used by each customer. Later successful logins were compared against this table.

### Learning

Fraud rules often need a reference/history table. For unknown device detection, the pipeline needs to remember the customer’s known devices.

---

## 8. Impossible Travel Logic

### Problem

The pipeline needed to detect cases where the same customer logged in from two distant countries within a very short time.

### Fix

Used latitude and longitude data with Snowflake geospatial functions:

```sql
ST_DISTANCE(
    ST_MAKEPOINT(previous_longitude, previous_latitude),
    ST_MAKEPOINT(current_longitude, current_latitude)
) / 1000 AS distance_kms
```

Then flagged records where the login time difference was very small and the distance was too large.

### Learning

Fraud detection can combine normal SQL logic with geospatial calculations to detect suspicious behavior.


---

## Summary of Learnings

Through these errors, I learned how to:

- Debug Snowflake stored procedures
- Handle batch-wise incremental processing
- Use metadata columns like `batch_id`
- Separate valid records from rejected records
- Use `TRY_CAST` and `TRY_TO_TIMESTAMP` for safe validation
- Apply SCD Type 2 logic for customer history
- Build fraud rules using SQL window functions
- Use Snowflake geospatial functions for impossible travel detection
- Orchestrate a multi-step data pipeline using Airflow

# Banking Fraud Detection Pipeline

An end-to-end **Data Engineering project** using **AWS S3, Snowflake, and Apache Airflow** to process banking transaction and login data, clean invalid records, maintain customer history, and detect suspicious fraud patterns.

---

## Project Overview

This project simulates a real banking fraud detection pipeline.

Banking data files are assumed to arrive in cloud storage. Snowflake loads the files into raw tables, transforms valid records into curated tables, and stores suspicious or invalid records in fraud tables. Apache Airflow orchestrates the full pipeline by calling Snowflake stored procedures in the correct order.

The pipeline focuses on:

- Loading raw transaction, login, customer, and coordinate data
- Cleaning and validating transaction records
- Cleaning and validating login records
- Capturing rejected records with flag and severity
- Detecting suspicious transaction amounts
- Detecting duplicate records
- Detecting repeated failed login attempts
- Detecting unknown device logins
- Detecting impossible travel based on login country distance
- Maintaining customer history using SCD Type 2 logic

---

## Tech Stack

| Tool | Purpose |
|---|---|
| AWS S3 | Stores incoming banking files |
| Snowflake | Raw, curated, and fraud detection data warehouse |
| Snowflake SQL | Data cleaning, validation, SCD Type 2, and fraud logic |
| Apache Airflow | Workflow orchestration |
| Python | Airflow DAG development |

---

## Architecture

```text
AWS S3
  ↓
Snowflake External Stage
  ↓
Raw Tables
  ↓
Snowflake Stored Procedures
  ↓
Curated Tables + Fraud Tables
  ↓
Airflow DAG Orchestration
```

---

## Repository Structure

```text
banking-fraud-detection-pipeline/
│
├── dags/
│   └── banking_fraud_dag.py
│
├── sql/
│   └── banking_fraud_procedures.sql
│
├── docs/
│   └── screenshots/
│
├── README.md
└── .gitignore
```

---

## What the Project Does

### 1. Raw Data Loading

The `coping` stored procedure loads data from Snowflake external stages into raw tables:

- `transactions_raw`
- `logins_raw`
- `customers_raw`
- `coordinates_raw`

It also captures metadata such as file name as `batch_id` so each pipeline run can process the latest batch.

---

### 2. Transaction Processing

The transaction procedures perform two major tasks.

#### Valid transaction load

Valid transaction records are inserted into `curated_data.transactions` after checking:

- Customer ID is present
- Transaction timestamp is valid
- Transaction type is allowed
- Amount is numeric and below the unusual threshold
- Merchant, city, country, device, and channel are valid
- Duplicate transaction IDs are removed

#### Invalid or suspicious transaction flags

Invalid records are inserted into `fraud_tb.transaction_flags` with a flag and severity.

Example flags:

- `INVALID_CUSTOMER`
- `INVALID_TXN_TIME`
- `INVALID_AMOUNT`
- `UNUSUAL_AMOUNT`
- `INVALID_COUNTRY`
- `DUPLICATE_RECORD`

---

### 3. Customer SCD Type 2 Logic

The customer procedure updates `curated_data.customers_scd` using SCD Type 2 logic.

When customer details change, the existing active record is marked as inactive, and a new active record is inserted.

This helps preserve customer history instead of overwriting old values.

---

### 4. Login Processing

The login procedures clean and validate login event data.

Valid login records are inserted into `curated_data.logins_curated` after checking:

- Customer ID is present
- Login timestamp is valid
- IP address format is valid
- Device ID is present
- Country is valid
- Login status is standardized
- Duplicate event IDs are handled

---

### 5. Fraud Detection Logic

The project detects multiple suspicious login patterns.

#### Duplicate login events

Duplicate login events are flagged and inserted into the fraud table.

#### Invalid login records

Rejected login records are flagged for reasons such as invalid customer, invalid date, invalid IP, invalid device, invalid country, or invalid login status.

#### Multiple failed logins

If a customer has multiple failed login attempts within a short time window, the record is flagged as high severity.

#### Unknown device login

The first known device for each customer is stored. Future successful logins from other devices are flagged as unknown-device activity.

#### Impossible travel

The impossible travel logic compares the previous login country and current login country using latitude and longitude. If the distance is too high for the short time difference, the login is flagged as suspicious.

---

## Airflow DAG Explanation

The Airflow DAG calls the Snowflake stored procedures in sequence.

```text
load_raw_files
  ↓
update_customers_scd
  ↓
curate_transactions
  ↓
flag_invalid_transactions
  ↓
load_known_devices
  ↓
curate_logins
  ↓
flag_duplicate_logins
  ↓
flag_invalid_logins
  ↓
flag_failed_login_burst
  ↓
flag_unknown_devices
  ↓
flag_impossible_travel
```

This makes the pipeline repeatable and automated.

---

## Key Data Engineering Concepts Demonstrated

- Batch ingestion from external stages
- Raw to curated data flow
- Data validation and cleansing
- Fraud rule implementation
- Deduplication using `ROW_NUMBER()`
- Incremental batch processing using `batch_id`
- SCD Type 2 customer history tracking
- Stored procedures in Snowflake
- SQL `MERGE` for idempotent inserts
- Airflow orchestration
- Separation of raw, curated, and fraud layers




---
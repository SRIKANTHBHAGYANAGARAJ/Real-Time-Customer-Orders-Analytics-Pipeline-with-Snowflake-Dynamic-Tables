# Real-Time Customer Orders Analytics Pipeline with Snowflake Dynamic Tables

A declarative data pipeline built in Snowflake that transforms raw customer and order data into analytics-ready fact tables — **without any external orchestration tools**.

![Snowflake](https://img.shields.io/badge/Snowflake-Dynamic%20Tables-29B5E8?logo=snowflake&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-Declarative-blue)
![Status](https://img.shields.io/badge/Status-Complete-brightgreen)
![License](https://img.shields.io/badge/License-MIT-yellow)

---

## 📌 Overview

Traditional data pipelines rely on Airflow, cron jobs, or stored procedures to manage dependencies and refresh schedules. This project demonstrates how **Snowflake Dynamic Tables** replace all of that with a **declarative SQL-only approach**.

You define the transformation. Snowflake handles:

- ✅ Dependency detection
- ✅ Refresh scheduling (`target_lag=downstream`)
- ✅ Pipeline monitoring
- ✅ Data quality enforcement

---

## 🏗️ Architecture

```
RAW_DB.CUSTOMERS ──▶ STG_CUSTOMERS_DT ──┐
                                        ├──▶ FCT_CUSTOMER_ORDERS_DT
RAW_DB.ORDERS    ──▶ STG_ORDERS_DT    ──┘
```

**Pipeline stages:**

1. **Staging layer** — column renaming, type casting, JSON unpacking
2. **Fact layer** — joins staging tables into analytics-ready output
3. **Monitoring** — refresh history + DAG visualization
4. **Data quality** — null filters enforced at table definition level

---

## 🧰 Tech Stack

| Tool | Purpose |
|------|---------|
| Snowflake | Data warehouse + Dynamic Tables |
| SQL | Declarative transformations |
| Snowsight | DAG visualization + monitoring |
| Git / GitHub | Version control |

---

## 🔑 Key Features

- ✅ **Declarative transformations** — no stored procedures
- ✅ **Automatic dependency management** via `target_lag=downstream`
- ✅ **JSON unpacking** — `purchase:"prodid"::number` → structured columns
- ✅ **Column renaming & type casting** in staging layer
- ✅ **Chained Dynamic Tables** — fact table auto-detects upstream deps
- ✅ **DAG visualization** in Snowsight
- ✅ **Refresh history monitoring** via `information_schema.dynamic_table_refresh_history()`
- ✅ **Data quality filters** embedded in table definitions
- ✅ **Adjustable refresh cadence** using `ALTER DYNAMIC TABLE`

---

## 📂 Project Structure

```
real-time-customer-orders-analytics-snowflake/
│
├── README.md
├── LICENSE
├── pipeline.sql               # all-in-one pipeline script
│
├── sql/                       # optional: split versions
│   ├── 01_create_staging_dts.sql
│   ├── 02_chain_fact_dt.sql
│   ├── 03_monitoring.sql
│   └── 04_data_quality.sql
│
├── img/
│   ├── dag_pipeline.png
│   ├── raw_orders_json.png
│   ├── stg_customers_table.png
│   ├── customers_by_name.png
│   ├── orders_by_date_bar.png
│   ├── orders_by_date_line_zigzag.png
│   ├── orders_by_date_line_smooth.png
│   ├── customer_id_linear.png
│   └── stockdate_pid.png
│
└── docs/
    └── architecture.md
```

---

## 📸 Pipeline Screenshots

### 1. Pipeline DAG (Directed Acyclic Graph)
*Flow: RAW CUSTOMERS → STG_CUSTOMERS_DT → FCT_CUSTOMER_ORDERS_DT*
![DAG Pipeline](img/dag_pipeline.png)

### 2. Raw Orders Table — JSON Data
*The `PURCHASE` column contains nested JSON that gets unpacked.*
![Raw Orders JSON](img/raw_orders_json.png)

### 3. Staging Customers Table
*Column renaming (`CUSTID` → `customer_id`) and type casting (`SPENDLIMIT` → FLOAT).*
![Staging Customers](img/stg_customers_table.png)

### 4. Customers by Name (Bar Chart)
![Customers by Name](img/customers_by_name.png)

### 5. Orders by Date (Bar Chart)
![Orders by Date Bar](img/orders_by_date_bar.png)

### 6. Orders by Date (Line Chart — Zigzag)
![Orders by Date Zigzag](img/orders_by_date_line_zigzag.png)

### 7. Orders by Date (Line Chart — Smooth)
![Orders by Date Smooth](img/orders_by_date_line_smooth.png)

### 8. Customer ID Trend (Linear)
![Customer ID Linear](img/customer_id_linear.png)

### 9. Product Stock Date (Bar Chart)
![Stock Date PID](img/stockdate_pid.png)

---

## 🚀 Getting Started

### Prerequisites

- Snowflake account (any edition supporting Dynamic Tables)
- `ACCOUNTADMIN` role (or equivalent privileges)
- Warehouse: `COMPUTE_WH`
- Database: `ANALYTICS_DB`
- Raw source tables: `RAW_DB.PUBLIC.CUSTOMERS`, `RAW_DB.PUBLIC.ORDERS`

### ⚡ Run Everything in One Shot

The complete pipeline is available as a single script:

```bash
# Open Snowsight → Worksheets → Paste pipeline.sql → Run All
```

Or via SnowSQL:

```bash
snowsql -a <account> -u <user> -f pipeline.sql
```

**File:** [`pipeline.sql`](./pipeline.sql)

This script:

1. Sets up the environment
2. Creates both staging Dynamic Tables
3. Chains them into a fact Dynamic Table
4. Enables monitoring via refresh history
5. Adjusts refresh frequency
6. Enforces data quality filters
7. Runs final validation queries

### Step 1 — Set Up Environment

```sql
use role accountadmin;
use warehouse compute_wh;
use database analytics_db;
```

### Step 2 — Create Staging Dynamic Tables

```sql
-- Customers staging table
create or replace dynamic table stg_customers_dt
    target_lag = downstream
    warehouse = compute_wh
    as select
        custid as customer_id,
        cname as customer_name,
        cast(spendlimit as float) as spend_limit
    from raw_db.public.customers;

-- Orders staging table (JSON unpacking)
create or replace dynamic table stg_orders_dt
    target_lag = downstream
    warehouse = compute_wh
    as select
        custid as customer_id,
        purchase:"prodid"::number(5) as product_id,
        purchase:"purchase_amount"::float(10) as order_price,
        purchase:"quantity"::number(5) as quantity,
        purchase:"purchase_date"::date as order_date
    from raw_db.public.orders;
```

### Step 3 — Chain Into Fact Table

```sql
create or replace dynamic table fct_customer_orders_dt
    target_lag = downstream
    warehouse = compute_wh
    as select
        c.customer_id,
        c.customer_name,
        o.product_id,
        o.order_price,
        o.quantity,
        o.order_date
    from stg_customers_dt c
    left join stg_orders_dt o
        on c.customer_id = o.customer_id
    where o.product_id is not null;
```

### Step 4 — Monitor Refresh History

```sql
select * from table(information_schema.dynamic_table_refresh_history());
```

### Step 5 — Adjust Refresh Frequency

```sql
alter dynamic table stg_orders_dt set target_lag = '5 minutes';
```

---

## 📊 Results & Insights

- **Real-time customer order analytics** — refreshed automatically
- **Zero external orchestration** — no Airflow, no cron, no stored procedures
- **Auto-refresh** with `target_lag=downstream`
- **Adjustable freshness** (e.g., `5 minutes`)
- **Built-in pipeline observability** via Snowsight DAG + refresh history

**Charts generated:**

- Orders by date (bar + line)
- Customers by name (bar)
- Customer spend limits (table)
- Product stock date trends (bar)

---

## 📖 What I Learned

- How to build Dynamic Tables with `target_lag=downstream`
- Column renaming, type casting, and JSON unpacking in declarative SQL
- Chaining Dynamic Tables by referencing upstream tables by name
- Visualizing pipelines using Snowflake's DAG view
- Monitoring via `information_schema.dynamic_table_refresh_history()`
- Enforcing data quality filters inside table definitions
- Tuning refresh cadence with `ALTER DYNAMIC TABLE ... SET TARGET_LAG`

---

## 🤝 Contributing

Contributions, issues, and feature requests are welcome.
Feel free to fork this repo and submit a pull request.

---

## 🔗 Connect

**LinkedIn:** [Your Name](https://linkedin.com/in/yourprofile)
**Project Title:** Real-Time Customer Orders Analytics Pipeline with Snowflake Dynamic Tables

---

## 📜 License

This project is licensed under the **MIT License** — see the [LICENSE](LICENSE) file for details.

---

## ⭐ Show Your Support

If you found this project useful, please give it a ⭐ on GitHub!

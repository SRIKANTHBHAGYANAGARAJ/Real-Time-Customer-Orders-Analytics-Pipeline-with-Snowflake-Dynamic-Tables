
# Real-Time Customer Orders Analytics Pipeline with Snowflake Dynamic Tables

A declarative data pipeline built in Snowflake that transforms raw customer and order data into analytics-ready fact tables — **without any external orchestration tools**.

![Snowflake][(https://img.shields.io/badge/Snowflake-Dynamic%20Tables-29B5E8?logo=snowflake&logoColor=white](https://developerbadges.snowflake.com/b7c3ac0c-d153-4768-b8e7-53af9a741f6e#acc.SQz1Z3fa))
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
├── screenshots/
│   ├── dag_pipeline.png
│   ├── staging_customers.png
│   ├── staging_orders_json.png
│   ├── fact_table.png
│   ├── refresh_history.png
│   └── charts/
│       ├── orders_by_date.png
│       ├── customers_by_name.png
│       └── stockdate_pid.png
│
└── docs/
    └── architecture.md
```

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

## 📸 Screenshots

### Pipeline DAG
### customers limit by python UDF
<img width="1411" height="363" alt="customers limit by python UDF" src="https://github.com/user-attachments/assets/01fe1add-6ad5-4204-a38c-269125c3a7b3" />

### customers limit by python UDF 1 : 
<img width="1427" height="356" alt="Customer limit by using python UDF1" src="https://github.com/user-attachments/assets/19e1c0e7-6726-489a-8718-a8384e4fc3db" />

### product table
<img width="1398" height="365" alt="product table" src="https://github.com/user-attachments/assets/d239e8ca-4d54-4504-9770-73d53dc4f493" />


### Orders limit by Customers
<img width="1420" height="371" alt="Orders limit by Customers" src="https://github.com/user-attachments/assets/73457af7-3933-4a01-a465-5e8f1bece797" />

### Create Staging Tables :
1)Exploring the Customers Table
<img width="1420" height="371" alt="Create Staging Tables" src="https://github.com/user-attachments/assets/24cb0daf-3aa5-449d-afe0-9425b5e8543b" />

### Verify the Dynamic Tables : 

<img width="1421" height="396" alt="Verify the Dynamic Tables" src="https://github.com/user-attachments/assets/66589d0c-24eb-4608-b6e1-1b6e9f63a3da" />

### new Face Model Dynamic Table :
<img width="1427" height="400" alt="new Face Model Dynamic Table" src="https://github.com/user-attachments/assets/644e6eb5-ea1a-43e3-aa4b-8842b95881e3" />

### ANALYTICS_DB.PUBLIC.FCT_CUSTOMER_ORDERS_DT

<img width="1153" height="445" alt="ANALYTICSDB PUBLIC FCT CUSTOMERORDERSDT" src="https://github.com/user-attachments/assets/15b951a6-58bd-4017-8de2-b181a72c66d9" />

### ANALYTICS_DB PUBLIC STG_CUSTOMERS_DT

<img width="1125" height="357" alt="ANALYTICS_DB PUBLIC STG_CUSTOMERS_DT" src="https://github.com/user-attachments/assets/c5cefc79-a151-4d84-a19d-88defc5b4771" />

### ANALYTICS_DB PUBLIC STG_ORDERS_DT
<img width="1202" height="350" alt="ORDERS" src="https://github.com/user-attachments/assets/99a225ef-ef62-4e50-92b6-129bf5cdab89" />


###Implement Data Quality
<img width="1172" height="351" alt="Data qualityimplentation" src="https://github.com/user-attachments/assets/b2f3b6ff-6b0d-4aa9-8331-3145e839f312" />

### Data quality enforcement by removing null orders
<img width="1157" height="376" alt="data quality enforcement by removing null orders" src="https://github.com/user-attachments/assets/46cc530d-e614-492d-a81b-18fc5611f446" />

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

**LinkedIn:** [Srikanth bn](https://www.linkedin.com/in/srikanth-b-n-9b4709288/)
**Project Title:** Real-Time Customer Orders Analytics Pipeline with Snowflake Dynamic Tables

---

## 📜 License

This project is licensed under the **MIT License** — see the [LICENSE](LICENSE) file for details.

---

## ⭐ Show Your Support

If you found this project useful, please give it a ⭐ on GitHub!

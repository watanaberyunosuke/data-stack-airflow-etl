# Airflow ETL Pipeline - Healthcare Analytics

This project is a work-in-progress healthcare data platform that models how a provider network could ingest, standardise, and transform operational data from outpatient pharmacy, telehealth, and partner clinic systems.

The runtime topology now separates the operational source system from the warehouse:

- `sales_oltp` or `POSTGRES_OLTP_DATABASE`: source-system database
- `sales_dwh` or `POSTGRES_DWH_DATABASE`: warehouse database
- warehouse schemas: `landing -> staging -> gold`

## Progress

- [x] Seeding module
- [ ] Spatial Information module: Base Spatial Information DWH
- [ ] Transform module: DBT
- [ ] Orchestration: Airflow
- [ ] Container: Docker Compose

## Domain Focus

The current sample domain is Australian community healthcare. The mock operational data represents:

- patient medication dispensing events
- medication master data by city
- care access channels such as walk-in, patient portal, and telehealth
- partner clinic and pharmacy flat-file feeds

## Seeding Module

The seeding module publishes a mock OLTP source system in one database and lands warehouse-ingestion tables in a separate warehouse database. It also generates CSV and XML landing files that emulate daily dispense feeds from external healthcare partners.

### Getting Started

1. Create a virtual environment using your preferred environment manager.

2. Install the required packages within the virtual environment:

```bash
pip install -r requirements.txt
```

3. Create a .env file based on the provided example (.env.example).
   Required values:

```bash
POSTGRES_HOST=localhost
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_OLTP_DATABASE=sales_oltp
POSTGRES_DWH_DATABASE=sales_dwh
```

4. Seed the database and generate healthcare source files by running:

```bash
python3 data-generate/initialise.py
```

5. Build the warehouse models in the warehouse database:

```bash
dbt build --project-dir dbt/dbt_med --profiles-dir dbt/dbt_med
```

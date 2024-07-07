# Airflow ETL Pipeline - Juice Shop

This is WIP

## Progress

- [x] Seeding module
- [ ] Spatial Information module: Base Spatial Information DWH
- [ ] Transform module: DBT
- [ ] Orchestration: Airflow
- [ ] Container: Docker Compose

## Seeding Module

The Seeding module is designed to publish a mock OLTP database, simulating business activities. Additionally, it generates flat files to emulate data provided by resellers.

### Getting Started

1. Create a virtual environment using your preferred environment manager.

2. Install the required packages within the virtual environment:

```bash
pip install -r requirements.txt
```

3. Create a .env file based on the provided example (.env.example).

4. Seed the database and generate flatfiles by running:

```bash
python3 data-generate/initialise.py
```

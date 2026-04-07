import csv
import os
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from random import choice, randrange

import declxml as xml
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from faker import Faker
from psycopg2.extras import execute_values

from assets import (
    ALL_DAYS,
    CSV_RESELLERS,
    FIRST_NAMES,
    LAST_NAMES,
    ORDER_METHOD,
    PRODUCTS,
    RESELLERS_TRANSACTIONS,
    XML_RESELLERS,
    random_date,
)

load_dotenv()

fake = Faker("en_AU")

FILE_LANDING_DIR = Path(__file__).resolve().parent / "file_landing"
DB_CONFIG = {
    "user": os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
    "host": os.environ["POSTGRES_HOST"],
    "port": "5432",
    "database": os.environ["POSTGRES_OLTP_DATABASE"],
}


def get_connection(search_path=None):
    connection_args = dict(DB_CONFIG)
    if search_path:
        connection_args["options"] = f"-c search_path={search_path}"
    return psycopg2.connect(**connection_args)


def generate_customer_id():
    """Generate a unique patient identifier."""
    return str(uuid.uuid4())


def build_patient_registry(n=1000):
    """Create a reusable patient registry shared by internal and partner feeds."""
    patients = []

    for _ in range(n):
        customer_id = generate_customer_id()
        first_name = choice(FIRST_NAMES)
        last_name = choice(LAST_NAMES)
        email_domain = fake.free_email_domain()

        patients.append(
            {
                "customer_id": customer_id,
                "first_name": first_name,
                "last_name": last_name,
                "email": f"{first_name}.{last_name}.{customer_id[:8]}@{email_domain}".lower(),
            }
        )

    return patients


def ensure_file_landing_directory():
    FILE_LANDING_DIR.mkdir(parents=True, exist_ok=True)


"""
    Generate and publish OLTP data
"""


def set_up_oltp_schema():
    print("Setting up OLTP schema...")

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("DROP SCHEMA IF EXISTS source CASCADE")
            cursor.execute("DROP SCHEMA IF EXISTS raw CASCADE")
            cursor.execute("DROP SCHEMA IF EXISTS staging CASCADE")

            cursor.execute("CREATE SCHEMA source")
            cursor.execute("CREATE SCHEMA raw")
            cursor.execute("CREATE SCHEMA staging")


def generate_oltp_data(patients, n=100000):
    print("Generating mock healthcare encounter data...")

    transactions_list = []
    order_method_ids = [item["order_method_id"] for item in ORDER_METHOD]

    for transaction_id in range(1, n + 1):
        product = choice(PRODUCTS)
        patient = choice(patients)
        transaction_date = random_date()
        quantity = randrange(1, 5)

        transactions_list.append(
            {
                "transaction_id": transaction_id,
                "customer_id": patient["customer_id"],
                "product_id": product["product_id"],
                "amount": round(product["price"] * quantity, 2),
                "quantity": quantity,
                "order_method_id": choice(order_method_ids),
                "transaction_date": transaction_date,
            }
        )

    return transactions_list


def publish_oltp_transactions(patients, n=100000):
    print("Loading OLTP dispensing events table...")
    transactions_list = generate_oltp_data(patients, n)
    if not transactions_list:
        print("No transactions generated.")
        return

    columns = list(transactions_list[0].keys())
    values = [list(transaction.values()) for transaction in transactions_list]

    with get_connection("raw") as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS source.transactions")
            cur.execute(
                """
                CREATE TABLE source.transactions (
                    transaction_id int primary key,
                    customer_id uuid,
                    product_id int,
                    amount numeric(12, 2),
                    quantity int,
                    order_method_id int,
                    transaction_date date,
                    load_timestamp timestamp default now()
                )
                """
            )

            query = "INSERT INTO source.transactions({}) VALUES %s".format(
                ",".join(columns)
            )
            execute_values(cur, query, values)
        conn.commit()

    print("Dispensing events table loaded successfully.")


def publish_oltp_order_methods():
    print("Loading OLTP care access channels table...")

    columns = list(ORDER_METHOD[0].keys())
    values = [list(method.values()) for method in ORDER_METHOD]

    with get_connection("raw") as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS source.order_methods")
            cur.execute(
                """
                CREATE TABLE source.order_methods (
                    order_method_id int,
                    order_method_name varchar(255)
                )
                """
            )

            query = "INSERT INTO source.order_methods({}) VALUES %s".format(
                ",".join(columns)
            )
            execute_values(cur, query, values)
        conn.commit()


def publish_oltp_customers(patients):
    print("Loading OLTP patients table...")

    columns = list(patients[0].keys())
    values = [list(patient.values()) for patient in patients]

    with get_connection("raw") as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS source.customers")
            cur.execute(
                """
                CREATE TABLE source.customers (
                    customer_id uuid,
                    first_name varchar(255),
                    last_name varchar(255),
                    email varchar(255)
                )
                """
            )

            query = "INSERT INTO source.customers({}) VALUES %s".format(
                ",".join(columns)
            )
            execute_values(cur, query, values)
        conn.commit()


def publish_oltp_resellers():
    print("Publishing OLTP healthcare partners table...")

    columns = list(RESELLERS_TRANSACTIONS[0].keys())
    values = [list(reseller.values()) for reseller in RESELLERS_TRANSACTIONS]

    with get_connection("raw") as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS source.resellers")
            cur.execute(
                """
                CREATE TABLE source.resellers (
                    reseller_id int,
                    reseller_name varchar(255),
                    commission_pct decimal
                )
                """
            )

            query = "INSERT INTO source.resellers({}) VALUES %s".format(
                ",".join(columns)
            )
            execute_values(cur, query, values)
        conn.commit()


def publish_oltp_resellers_csv():
    print("Loading partner CSV feeds into source.resellerscsv...")
    ensure_file_landing_directory()

    expected_columns = [
        "transaction_id",
        "reseller_id",
        "product_name",
        "quantity",
        "total_amount",
        "order_method",
        "customer_id",
        "customer_first_name",
        "customer_last_name",
        "city",
        "transaction_date",
    ]
    insert_columns = expected_columns + ["imported_file"]

    with get_connection("raw") as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS source.resellerscsv")
            cur.execute(
                """
                CREATE TABLE source.resellerscsv (
                    transaction_id int,
                    reseller_id int,
                    product_name varchar(255),
                    quantity int,
                    total_amount numeric(12, 2),
                    order_method varchar(255),
                    customer_id uuid,
                    customer_first_name varchar(255),
                    customer_last_name varchar(255),
                    city varchar(255),
                    transaction_date date,
                    imported_file varchar(255),
                    load_timestamp timestamp default now()
                )
                """
            )

            for filepath in sorted(FILE_LANDING_DIR.glob("*.csv")):
                try:
                    df = pd.read_csv(filepath)
                    missing_columns = [
                        column for column in expected_columns if column not in df.columns
                    ]
                    if missing_columns:
                        print(
                            f"Skipping {filepath.name} due to missing columns: {missing_columns}"
                        )
                        continue

                    for column in ["transaction_id", "reseller_id", "quantity"]:
                        df[column] = df[column].astype(int)
                    df["total_amount"] = df["total_amount"].astype(float)
                    df["transaction_date"] = pd.to_datetime(
                        df["transaction_date"]
                    ).dt.date
                    df["imported_file"] = filepath.name

                    records = [
                        tuple(row)
                        for row in df[insert_columns].itertuples(index=False, name=None)
                    ]
                    query = f"""
                        INSERT INTO source.resellerscsv ({", ".join(insert_columns)})
                        VALUES %s
                    """
                    execute_values(cur, query, records)
                    print(f"Loaded data from {filepath.name} successfully.")
                except Exception as exc:
                    print(f"Error processing {filepath.name}: {exc}")
        conn.commit()


def publish_preprocessed_resellers_xml():
    print("Loading preprocessed XML partner extracts...")
    ensure_file_landing_directory()

    records = []

    for filepath in sorted(FILE_LANDING_DIR.glob("*.xml")):
        try:
            root = ET.parse(filepath).getroot()
        except ET.ParseError as exc:
            print(f"Skipping {filepath.name} due to XML parse error: {exc}")
            continue

        for transaction in root.findall("transaction"):
            customer = transaction.find("customer")
            create_date = transaction.findtext("createDate")
            customer_id = customer.findtext("customer_id") if customer is not None else None
            customer_first_name = (
                customer.findtext("first_name") if customer is not None else None
            )
            customer_last_name = (
                customer.findtext("last_name") if customer is not None else None
            )

            records.append(
                (
                    int(transaction.attrib["resellerId"]),
                    int(transaction.findtext("transactionId")),
                    transaction.findtext("productName"),
                    int(transaction.findtext("quantity")),
                    float(transaction.findtext("totalAmount")),
                    int(transaction.findtext("orderMethod")),
                    customer_id,
                    customer_first_name,
                    customer_last_name,
                    transaction.findtext("city"),
                    datetime.strptime(create_date, "%Y%m%d").date(),
                    filepath.name,
                )
            )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS staging.resellersxmlextracted")
            cur.execute(
                """
                CREATE TABLE staging.resellersxmlextracted (
                    reseller_id int,
                    transaction_id int,
                    product_name varchar(255),
                    quantity int,
                    total_amount numeric(12, 2),
                    order_method_id int,
                    customer_id uuid,
                    customer_first_name varchar(255),
                    customer_last_name varchar(255),
                    city varchar(255),
                    transaction_date date,
                    imported_file varchar(255),
                    load_timestamp timestamp default now()
                )
                """
            )

            if records:
                execute_values(
                    cur,
                    """
                    INSERT INTO staging.resellersxmlextracted (
                        reseller_id,
                        transaction_id,
                        product_name,
                        quantity,
                        total_amount,
                        order_method_id,
                        customer_id,
                        customer_first_name,
                        customer_last_name,
                        city,
                        transaction_date,
                        imported_file
                    ) VALUES %s
                    """,
                    records,
                )
        conn.commit()


def publish_oltp_products():
    print("Publishing OLTP medication catalogue table...")

    if not PRODUCTS:
        print("No medication catalogue rows to insert.")
        return

    columns = list(PRODUCTS[0].keys())
    values = [list(product.values()) for product in PRODUCTS]

    with get_connection("raw") as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS source.products")
            cur.execute(
                """
                CREATE TABLE source.products (
                    product_id int primary key,
                    product_name varchar(255),
                    city varchar(255),
                    price numeric(12, 2)
                )
                """
            )

            query = "INSERT INTO source.products({}) VALUES %s".format(
                ",".join(columns)
            )
            execute_values(cur, query, values)
        conn.commit()

    print("Medication catalogue table loaded successfully.")


"""
    Create and generate CSV files
"""


def generate_csv_data(patients, reseller_id, n):
    print(f"Generating healthcare CSV feed data for partner {reseller_id}...")

    export = []

    for _ in range(n):
        patient = choice(patients)
        product = choice(PRODUCTS)
        quantity = randrange(1, 10)
        order_method = choice(ORDER_METHOD)["order_method_name"]
        transaction_date = random_date().isoformat()

        export.append(
            {
                "reseller_id": reseller_id,
                "product_name": product["product_name"],
                "quantity": quantity,
                "total_amount": round(quantity * product["price"], 2),
                "order_method": order_method,
                "customer_id": patient["customer_id"],
                "customer_first_name": patient["first_name"],
                "customer_last_name": patient["last_name"],
                "city": product["city"],
                "transaction_date": transaction_date,
            }
        )

    return export


def create_csv_file(patients, n):
    print("Creating CSV healthcare partner files...")
    ensure_file_landing_directory()

    for reseller_id in CSV_RESELLERS:
        export = generate_csv_data(patients, reseller_id, n)
        if not export:
            continue

        keys = ["transaction_id"] + list(export[0].keys())
        transaction_id = 1

        for day in ALL_DAYS:
            data = [transaction for transaction in export if transaction["transaction_date"] == day]
            if not data:
                continue

            for entry in data:
                entry["transaction_id"] = transaction_id
                transaction_id += 1

            year, month, day_of_month = day.split("-")
            file_date = f"{year}{day_of_month}{month}"
            file_path = FILE_LANDING_DIR / f"DailyDispense_{file_date}_{reseller_id}.csv"

            with file_path.open("w", newline="") as output_file:
                dict_writer = csv.DictWriter(output_file, keys)
                dict_writer.writeheader()
                dict_writer.writerows(data)


"""
    Create and generate XML file
"""


def generate_xml_data(patients, reseller_id, n=5):
    print(f"Generating healthcare XML feed data for partner {reseller_id}...")
    export = []

    for _ in range(n):
        patient = choice(patients)
        product = choice(PRODUCTS)
        quantity = randrange(1, 10)
        order_method = choice(ORDER_METHOD)

        transaction_date = random_date().strftime("%Y%m%d")
        export.append(
            {
                "date": transaction_date,
                "resellerId": reseller_id,
                "productName": product["product_name"],
                "orderMethod": order_method["order_method_id"],
                "quantity": quantity,
                "totalAmount": round(quantity * product["price"], 2),
                "customer": {
                    "customer_id": patient["customer_id"],
                    "first_name": patient["first_name"],
                    "last_name": patient["last_name"],
                },
                "createDate": transaction_date,
                "city": product["city"],
            }
        )

    return export


def create_xml_file(patients, n=25):
    print("Creating XML healthcare partner files...")
    ensure_file_landing_directory()

    transaction_processor = xml.dictionary(
        "transaction",
        [
            xml.string(".", attribute="date"),
            xml.integer(".", attribute="resellerId"),
            xml.integer("transactionId"),
            xml.string("productName"),
            xml.integer("orderMethod"),
            xml.integer("quantity"),
            xml.floating_point("totalAmount"),
            xml.dictionary(
                "customer",
                [
                    xml.string("customer_id"),
                    xml.string("first_name"),
                    xml.string("last_name"),
                ],
            ),
            xml.string("createDate"),
            xml.string("city"),
        ],
    )

    for reseller_id in XML_RESELLERS:
        transaction_id = 1
        export = generate_xml_data(patients, reseller_id, n=n)
        if not export:
            continue

        for day in ALL_DAYS:
            day_formatted = day.replace("-", "")
            data = [transaction for transaction in export if transaction["createDate"] == day_formatted]
            if not data:
                continue

            for entry in data:
                entry["transactionId"] = transaction_id
                transaction_id += 1

            lines = ['<?xml version="1.0" encoding="utf-8"?>', "<transactions>"]

            for transaction in data:
                xml_str = xml.serialize_to_string(
                    transaction_processor, transaction, indent="  "
                )
                lines.extend(xml_str.split("\n")[1:])

            lines.append("</transactions>")

            year = day_formatted[0:4]
            month = day_formatted[4:6]
            day_of_month = day_formatted[6:8]
            file_date = f"{year}{day_of_month}{month}"
            file_path = FILE_LANDING_DIR / f"DailyDispense_{file_date}_{reseller_id}.xml"

            with file_path.open("w", newline="") as output_file:
                output_file.write("\n".join(lines))


def clean_up(directory, ext):
    target_directory = Path(directory)
    if not target_directory.exists():
        return

    for file in target_directory.glob(f"*.{ext}"):
        file.unlink()

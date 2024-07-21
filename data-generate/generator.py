import csv
import os
import uuid
from random import choice, randrange
from faker import Faker
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import declxml as xml
from assets import (
    ALL_DAYS,
    CITIES_RANGE,
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

CONNECTION = psycopg2.connect(
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
    host=os.environ["POSTGRES_HOST"],
    port="5432",
    database=os.environ["POSTGRES_OLTP_DATABASE"],
    options="-c search_path=extracts",
)

fake = Faker()

"""
    Generate and publish OLTP data
"""


def set_up_oltp_schema():
    print("Setting Up OLTP Schema...")

    with CONNECTION as conn:
        with conn.cursor() as cursor:
            # Drop Schema
            cursor.execute("DROP SCHEMA IF EXISTS extracts CASCADE")
            cursor.execute("DROP SCHEMA IF EXISTS source CASCADE")
            cursor.execute("DROP SCHEMA IF EXISTS raw CASCADE")
            cursor.execute("DROP SCHEMA IF EXISTS staging CASCADE")

            # Create Schema
            cursor.execute("CREATE SCHEMA extracts")
            cursor.execute("CREATE SCHEMA source")
            cursor.execute("CREATE SCHEMA raw")
            cursor.execute("CREATE SCHEMA staging")


def generate_customer_id():
    """
    Generate a unique customer ID using UUID.
    """
    return str(uuid.uuid4())


def generate_oltp_data(n=100000):
    print("Generating mock data OLTP...")

    transactions_list = []
    order_method_id_list = [i["order_method_id"] for i in ORDER_METHOD]

    for i in range(n):
        product = choice(PRODUCTS)
        transaction_date = random_date()
        if not transaction_date:
            continue  # Skip if the transaction date is invalid
        transaction_date = str(transaction_date)
        quantity = randrange(1, 5)

        transaction_info = {
            "transaction_id": randrange(1, 100000),
            "customer_id": generate_customer_id(),
            "product_id": product["product_id"],
            "amount": product["price"] * quantity,
            "quantity": quantity,
            "order_method_id": choice(order_method_id_list),
            "transaction_date": transaction_date,
        }
        transactions_list.append(transaction_info)

    if not transactions_list:
        print("No transactions generated.")

    return transactions_list


def publish_oltp_transactions(n=100000):
    print("Loading OLTP Transactions Table...")
    transactions_list = generate_oltp_data(n)
    if not transactions_list:
        print("No transactions generated.")
        return

    columns = transactions_list[0].keys()

    try:
        conn = psycopg2.connect(
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            host=os.environ["POSTGRES_HOST"],
            port="5432",
            database=os.environ["POSTGRES_OLTP_DATABASE"],
            options="-c search_path=extracts",
        )
        conn.autocommit = True

        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS transactions")
        cur.execute(
            "CREATE TABLE transactions(transaction_id serial primary key, customer_id uuid, product_id int, amount numeric(12, 2), quantity int, order_method_id int, transaction_date date, load_timestamp timestamp)"
        )

        query = "INSERT INTO transactions({}) VALUES %s".format(','.join(columns))
        values = [list(transaction.values()) for transaction in transactions_list]

        execute_values(cur, query, values)

        conn.commit()
        print("Transactions table loaded successfully.")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the cursor and connection
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


def publish_oltp_order_methods():
    print("Loading OLTP Order Methods Table...")

    columns = ORDER_METHOD[0].keys()

    with CONNECTION as conn:
        cur = conn.cursor()

        cur.execute("DROP TABLE IF EXISTS order_methods")
        cur.execute("CREATE TABLE order_methods(order_method_id int, order_method_name varchar(255))")

        query = "INSERT INTO order_methods({}) VALUES %s".format(','.join(columns))

        values = [list(transaction.values()) for transaction in ORDER_METHOD]

        execute_values(cur, query, values)

        conn.commit()


def publish_oltp_customers():
    print("Loading OLTP Customers Table...")

    customers_list = []

    for i in range(1, 1000):
        customer_id = generate_customer_id()
        first_name = choice(FIRST_NAMES)
        last_name = choice(LAST_NAMES)
        email = f"{first_name}.{last_name}@{list(fake.ascii_free_email().split('@'))[1]}"
        customers_list.append(
            {'customer_id': customer_id, 'first_name': first_name, 'last_name': last_name, 'email': email}
        )

    columns = customers_list[0].keys()

    with CONNECTION as conn:
        cur = conn.cursor()

        cur.execute("DROP TABLE IF EXISTS customers")
        cur.execute(
            "CREATE TABLE customers(customer_id uuid, first_name varchar(255), last_name varchar(255), email varchar(255))"
        )

        query = "INSERT INTO customers({}) VALUES %s".format(','.join(columns))

        values = [list(customer.values()) for customer in customers_list]

        execute_values(cur, query, values)

        conn.commit()


def publish_oltp_resellers():
    print("Publishing OLTP Resellers Table...")

    columns = RESELLERS_TRANSACTIONS[0].keys()

    with CONNECTION as conn:
        cur = conn.cursor()

        cur.execute("DROP TABLE IF EXISTS resellers")
        cur.execute("CREATE TABLE resellers(reseller_id int, reseller_name varchar(255), commission_pct decimal)")

        query = "INSERT INTO resellers({}) VALUES %s".format(','.join(columns))

        values = [list(reseller.values()) for reseller in RESELLERS_TRANSACTIONS]

        execute_values(cur, query, values)

        conn.commit()


# TODO: Refine Resellerscsv table
def publish_oltp_resellers_csv():
    print("Publishing OLTP Resellers CSV Table...")

    with CONNECTION as conn:
        cur = conn.cursor()

        cur.execute("DROP TABLE IF EXISTS resellerscsv")
        cur.execute(
            "CREATE TABLE resellerscsv(reseller_id int, transaction_id int, product_name varchar(255), quantity int, total_amount numeric(12, 2), "
            "order_method varchar(255), customer_id uuid, customer_first_name varchar(255), customer_last_name varchar(255), "
            "city varchar(255), transaction_date date, load_timestamp timestamp)"
        )

        conn.commit()


def publish_oltp_products():
    print("Publishing OLTP Products Table...")

    product_list = PRODUCTS
    if not product_list:
        print("No products to insert.")
        return

    columns = product_list[0].keys()

    try:
        with CONNECTION as conn:
            cur = conn.cursor()

            cur.execute("DROP TABLE IF EXISTS products")
            cur.execute(
                "CREATE TABLE products(product_id int primary key, product_name varchar(255), city varchar(255), price numeric(12,2))"
            )

            query = "INSERT INTO products({}) VALUES %s".format(','.join(columns))
            values = [list(product.values()) for product in product_list]

            execute_values(cur, query, values)

            conn.commit()
            print("Products table loaded successfully.")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the cursor and connection
        if cur is not None:
            cur.close()


"""
    Create and generate CSV files
"""


def generate_csv_data(n):
    print('Generating CSV file data...')

    export = []

    for i in range(n):
        reseller_id = choice(RESELLERS_TRANSACTIONS)['reseller_id']
        product = choice(PRODUCTS)
        quantity = randrange(1, 10)
        order_method = choice(ORDER_METHOD)['order_method_id']
        transaction_date = str(random_date())
        if not transaction_date.strip():
            continue
        city = choice(CITIES_RANGE)
        first_name = choice(FIRST_NAMES)
        last_name = choice(LAST_NAMES)
        customer_id = generate_customer_id()
        

        transaction = {
            'reseller_id': reseller_id,
            'product_name': product['product_name'],
            'quantity': quantity,
            'total_amount': round(quantity * product['price'], 2),
            'order_method': order_method,
            'customer_id': customer_id,
            'city': city,
            'transaction_date': transaction_date,
        }

        export.append(transaction)

    return export


def create_csv_file(n):
    print('Creating CSV files...')
    for reseller_id in CSV_RESELLERS:

        export = generate_csv_data(n)
        if not export:
            continue

        # Include transaction_id in the keys
        keys = ['transaction_id'] + list(export[0].keys())
        transaction_id = 0

        for day in ALL_DAYS:
            data = [tran for tran in export if tran.get('transaction_date') == day]

            for entry in data:
                entry['transaction_id'] = transaction_id
                transaction_id += 1

            date_name_format = day.split('-')
            new_format = date_name_format[0] + date_name_format[2] + date_name_format[1]

            directory = 'data-generate/file_landing'
            if not os.path.exists(directory):
                os.makedirs(directory)
                print(f'Directory created at {directory}')

            file_path = f'{directory}/DailySales_{new_format}_{reseller_id}.csv'

            with open(file_path, 'w', newline='') as output_file:
                dict_writer = csv.DictWriter(output_file, keys)
                dict_writer.writeheader()
                dict_writer.writerows(data)


"""
    Create and generate XML file
"""


def generate_xml_data(reseller_id, n=5):
    print('Generating XML data...')
    export = []

    for i in range(n):
        product = choice(PRODUCTS)
        quantity = randrange(1, 10)
        order_method = choice(ORDER_METHOD)

        transaction_date = str(random_date())
        if not transaction_date.strip():
            continue  # Skip if the transaction date is empty
        transaction_date_formatted = transaction_date.replace('-', '')
        city = choice(CITIES_RANGE)

        customer_id = generate_customer_id()

        transaction = {
            'date': transaction_date_formatted,
            'resellerId': reseller_id,
            'productName': product['product_name'],
            'orderMethod': order_method['order_method_id'],
            'quantity': quantity,
            'totalAmount': quantity * product['price'] * 1.0,
            'customer': {
                'customer_id': customer_id
            },
            'createDate': transaction_date_formatted,
            'city': city,
        }

        export.append(transaction)
    return export


def create_xml_file():
    print('Create XML file...')

    transaction_processor = xml.dictionary(
        'transaction',
        [
            xml.string('.', attribute='date'),
            xml.integer('.', attribute='resellerId'),
            xml.integer('transactionId'),
            xml.string('productName'),
            xml.integer('orderMethod'),
            xml.integer('quantity'),
            xml.floating_point('totalAmount'),
            xml.dictionary('customer', [xml.string('customer_id')]),
            xml.string('createDate'),
            xml.string('city'),
        ],
    )

    for reseller_id in XML_RESELLERS:
        transaction_id = 0

        export = generate_xml_data(reseller_id)

        if not export:
            continue  # Skip if no data is generated for the reseller

        for day in ALL_DAYS:
            day_formatted = day.replace('-', '')
            data = [tran for tran in export if tran.get('createDate') == day_formatted]

            if not data:
                continue  # Skip if no data for the day

            for entry in data:
                entry['transactionId'] = transaction_id
                transaction_id += 1

            result = []

            result.append('<?xml version="1.0" encoding="utf-8"?>')
            result.append('<transactions>')

            for transaction in data:
                xml_str = xml.serialize_to_string(transaction_processor, transaction, indent='  ')
                splitted = xml_str.split('\n')
                result += splitted[1:]

            result.append('</transactions>')

            date_name_format = day.split('-')
            new_date_name_format = date_name_format[0] + date_name_format[2] + date_name_format[1]

            directory = 'data-generate/file_landing'
            file_path = f'{directory}/DailySales_{new_date_name_format}_{reseller_id}.xml'

            if not os.path.exists(directory):
                os.makedirs(directory)

            with open(file_path, 'w', newline='') as output_file:
                output_file.write('\n'.join(result))


def clean_up(directory, ext):
    filelist = [file for file in os.listdir(directory) if file.endswith(ext)]

    for file in filelist:
        os.remove(os.path.join(directory, file))

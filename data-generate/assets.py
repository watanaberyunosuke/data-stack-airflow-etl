from datetime import date, datetime, timedelta
import itertools
from random import randint, randrange
import random
import time

from faker import Faker

# Faker
fake = Faker()

# Define date ranges for random date generation
start_date_limit = date(2024, 6, 1)
current_datetime = date.today()
end_date_limit = date(
    current_datetime.year, current_datetime.month, current_datetime.day
)
time_delta = end_date_limit - start_date_limit

# Generate all days between the start and end dates
ALL_DAYS = [
    str(start_date_limit + timedelta(days=i)) for i in range(time_delta.days + 1)
]

# Define city and product ranges
CITIES_RANGE = [
    "Canberra",
    "Sydney",
    "Melbourne",
    "Brisbane",
    "Adelaide",
    "Perth",
    "Darwin",
    "Geelong",
    "Townsville",
    "Gold Coast",
    "Sunshine Coast",
    "Coffs Harbour",
    "Wollongong",
    "Cairns",
]
# Sample medication names
MEDICATIONS = [
    "Paracetamol",
    "Ibuprofen",
    "Metformin",
    "Amlodipine",
    "Atorvastatin",
    "Simvastatin",
    "Losartan",
    "Omeprazole",
    "Lisinopril",
    "Metoprolol",
    "Clopidogrel",
    "Duloxetine",
    "Ranitidine",
    "Hydrochlorothiazide",
    "Gabapentin",
]

PRODUCT_NAMES = random.choices(MEDICATIONS, k=10)

# Define order methods
ORDER_METHOD = [
    {"order_method_id": 1, "order_method_name": "Walk In"},
    {"order_method_id": 2, "order_method_name": "Official App"},
    {"order_method_id": 3, "order_method_name": "Other App"},
    {"order_method_id": 4, "order_method_name": "Web"},
]

# Initialise products list
PRODUCTS = []

# Generate random first and last names
FIRST_NAMES = [fake.first_name() for _ in range(1000)]
LAST_NAMES = [fake.last_name() for _ in range(1000)]

# Define reseller transactions
RESELLERS_TRANSACTIONS = [
    {"reseller_id": 1001, "reseller_name": "Chemist Lake", "commission_pct": 0.1},
    {"reseller_id": 1002, "reseller_name": "Terry Black", "commission_pct": 0.17},
    {
        "reseller_id": 1003,
        "reseller_name": "MoneyLine",
        "commission_pct": 0.14,
    },
    {"reseller_id": 1004, "reseller_name": "Slade", "commission_pct": 0.16},
]

XML_RESELLERS = [1001, 1002]
CSV_RESELLERS = [1003, 1004]


def random_date():
    result = start_date_limit + timedelta(
        seconds=randint(0, int((end_date_limit - start_date_limit).total_seconds()))
    )
    return result


# Generate product data by combining product names and cities
product_data = list(itertools.product(PRODUCT_NAMES, CITIES_RANGE))

PRODUCT_ID = 1

for product_name, city in product_data:
    product = {
        "product_name": product_name,
        "city": city,
        "price": randrange(60, 100) / 10.0,
        "product_id": PRODUCT_ID,
    }
    PRODUCTS.append(product)
    PRODUCT_ID += 1

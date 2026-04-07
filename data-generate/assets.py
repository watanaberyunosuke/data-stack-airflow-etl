from datetime import date, timedelta
import itertools
from random import randint, randrange
import random

from faker import Faker

# Faker
fake = Faker("en_AU")

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

# Define care network coverage
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
# Sample medication and treatment catalogue
MEDICATIONS = [
    "Paracetamol 500mg",
    "Ibuprofen 200mg",
    "Metformin XR 500mg",
    "Amlodipine 5mg",
    "Atorvastatin 20mg",
    "Losartan 50mg",
    "Omeprazole 20mg",
    "Lisinopril 10mg",
    "Metoprolol 50mg",
    "Clopidogrel 75mg",
    "Duloxetine 60mg",
    "Gabapentin 300mg",
    "Amoxicillin 500mg",
    "Sertraline 50mg",
    "Salbutamol Inhaler",
]

PRODUCT_NAMES = random.sample(MEDICATIONS, k=10)

# Define patient access channels
ORDER_METHOD = [
    {"order_method_id": 1, "order_method_name": "Walk-in Clinic"},
    {"order_method_id": 2, "order_method_name": "Patient Portal"},
    {"order_method_id": 3, "order_method_name": "Telehealth Referral"},
    {"order_method_id": 4, "order_method_name": "Hospital Discharge"},
]

# Initialise medication catalogue
PRODUCTS = []

# Generate random first and last names
FIRST_NAMES = [fake.first_name() for _ in range(1000)]
LAST_NAMES = [fake.last_name() for _ in range(1000)]

# Define external healthcare partner feeds
RESELLERS_TRANSACTIONS = [
    {
        "reseller_id": 1001,
        "reseller_name": "Harbour Family Clinic",
        "commission_pct": 0.1,
    },
    {
        "reseller_id": 1002,
        "reseller_name": "Northside Telehealth Hub",
        "commission_pct": 0.17,
    },
    {
        "reseller_id": 1003,
        "reseller_name": "Community Care Network",
        "commission_pct": 0.14,
    },
    {
        "reseller_id": 1004,
        "reseller_name": "Regional Day Hospital",
        "commission_pct": 0.16,
    },
]

XML_RESELLERS = [1001, 1002]
CSV_RESELLERS = [1003, 1004]


def random_date():
    result = start_date_limit + timedelta(
        seconds=randint(0, int((end_date_limit - start_date_limit).total_seconds()))
    )
    return result


# Generate medication availability by city
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

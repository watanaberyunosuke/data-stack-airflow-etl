import os
from pyspark.sql import SparkSession
import psycopg2
import glob

# Initialize Spark session
spark = SparkSession.builder.appName("InferAndLoadSchema").getOrCreate()

# Path to the CSV file
csv_file = "australian_postcodes.csv"

# Read the CSV file and infer schema
df = spark.read.csv(csv_file, header=True, inferSchema=True)

# Database connection properties
CONNECTION = psycopg2.connect(
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
    host=os.environ["POSTGRES_HOST"],
    port="5432",
    database=os.environ["POSTGRES_OLTP_DATABASE"],
)


# Function to map Spark SQL types to SQL types
def map_spark_dtype(dtype):
    if dtype.startswith("IntegerType"):
        return "BIGINT"
    elif dtype.startswith("LongType"):
        return "BIGINT"
    elif dtype.startswith("DoubleType"):
        return "FLOAT"
    elif dtype.startswith("BooleanType"):
        return "BOOLEAN"
    elif dtype.startswith("TimestampType"):
        return "TIMESTAMP"
    elif dtype.startswith("DateType"):
        return "DATE"
    else:
        return "VARCHAR"


# Infer schema and create SQL schema string
schema = df.dtypes
column_types = {col: map_spark_dtype(dtype) for col, dtype in schema}
columns = ", ".join([f"{col} {dtype}" for col, dtype in column_types.items()])

# Create table SQL statement for overall table
create_table_sql = f"CREATE TABLE public.australian_postcodes_overall ({columns});"

# Connect to PostgreSQL
conn = CONNECTION
conn.autocommit = True
cur = conn.cursor()

# Execute the SQL to create the overall table
cur.execute("DROP TABLE IF EXISTS public.australian_postcodes_overall")
cur.execute(create_table_sql)

# Write the DataFrame to a temporary directory as CSV
temp_dir = "/tmp/spark_temp"
df.write.csv(temp_dir, header=True, mode="overwrite")

# Find the CSV file written by Spark
csv_files = glob.glob(f"{temp_dir}/part-*.csv")
if len(csv_files) == 0:
    raise FileNotFoundError("No CSV files found in the temporary directory")

# Load the CSV into PostgreSQL
copy_sql = """
           COPY public.australian_postcodes_overall FROM stdin WITH CSV HEADER
           DELIMITER as ','
           """
for csv_file in csv_files:
    with open(csv_file, "r") as f:
        cur.copy_expert(sql=copy_sql, file=f)

# Create the new table with selected columns
create_new_table_sql = """
    CREATE TABLE public.australian_postcodes AS
    SELECT DISTINCT postcode, sa1_name_2021 as suburb, state, status
    FROM public.australian_postcodes_overall
    WHERE sa1_name_2021 IS NOT NULL AND type IS NOT NULL
    ORDER BY postcode;
"""

cur.execute("DROP TABLE IF EXISTS public.australian_postcodes")
cur.execute(create_new_table_sql)

# Close connections
cur.close()
conn.close()

# Stop Spark session
spark.stop()

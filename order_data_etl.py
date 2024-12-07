from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, from_json
import sqlite3
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Sales Data Processing") \
    .getOrCreate()

# Function to read data
def read_data(file_path, region):
    """
    Reads a CSV file and adds a 'region' column.
    """
    df = spark.read.option("quote", "\"") \
                   .option("escape", "\"") \
                   .csv(file_path, header=True, inferSchema=True)
    return df.withColumn("region", lit(region))

# Save DataFrame to SQLite
def save_to_sqlite(df, db_name, table_name):
    """
    Saves a PySpark DataFrame to SQLite.
    """
    conn = sqlite3.connect(db_name)
    df.toPandas().to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()

# Function to execute SQL queries for validation
def execute_query(query, db_name="sales_data.db"):
    """
    Executes a SQL query and returns the result.
    """
    conn = sqlite3.connect(db_name)
    result = conn.execute(query).fetchall()
    conn.close()
    return result

# Paths to the CSV files
file_a = "./data/order_region_a.csv"
file_b = "./data/order_region_b.csv"

# Read data from files
data_a = read_data(file_a, "A")
data_b = read_data(file_b, "B")

# Combine data from both regions
combined_data = data_a.union(data_b)

# Add 'total_sales' column
combined_data = combined_data.withColumn("total_sales", col("QuantityOrdered") * col("ItemPrice"))

# Define schema for PromotionDiscount
promo_discount_schema = StructType([
    StructField("CurrencyCode", StringType(), True),
    StructField("Amount", StringType(), True)  # Amount is stored as string, we'll convert it to double
])

# Parse the JSON column and extract 'Amount' as a numeric value
combined_data = combined_data.withColumn("PromotionDiscountAmount", 
                                         from_json(col("PromotionDiscount"), promo_discount_schema).getItem("Amount").cast(DoubleType()))


# Add 'net_sale' column
combined_data = combined_data.withColumn("net_sale", col("total_sales") - col("PromotionDiscountAmount"))

# Remove duplicate entries based on OrderId
combined_data = combined_data.dropDuplicates(["OrderId"])

# Filter out rows with negative or zero 'net_sale'
combined_data = combined_data.filter(col("net_sale") > 0)

# Save transformed data to SQLite
db_name = "sales_data.db"
table_name = "sales_data"
save_to_sqlite(combined_data, db_name, table_name)

# Validation Queries
total_records_query = "SELECT COUNT(*) FROM sales_data"
total_sales_query = "SELECT region, SUM(net_sale) AS total_sales FROM sales_data GROUP BY region"
average_sales_query = "SELECT AVG(net_sale) AS avg_sales FROM sales_data"
unique_order_ids_query = "SELECT COUNT(DISTINCT OrderId) FROM sales_data"

# Print Validation Results
print("Total Records:", execute_query(total_records_query))
print("Total Sales by Region:", execute_query(total_sales_query))
print("Average Sales per Transaction:", execute_query(average_sales_query))
print("Unique Order IDs:", execute_query(unique_order_ids_query))

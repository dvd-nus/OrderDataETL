from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import sqlite3

# Constants
DB_NAME = "sales_data.db"
TABLE_NAME = "sales_data"
FILE_PATHS = {
    "A": "./data/order_region_a.csv",
    "B": "./data/order_region_b.csv"
}

# Initialize Spark Session
def initialize_spark(app_name="Sales Data Processing"):
    """Initializes and returns a Spark Session."""
    return SparkSession.builder.appName(app_name).getOrCreate()

# Data Reading and Transformation Functions
def read_and_tag_data(spark, file_path, region):
    """Reads a CSV file and adds a 'region' column."""
    return spark.read.option("quote", "\"") \
                     .option("escape", "\"") \
                     .csv(file_path, header=True, inferSchema=True) \
                     .withColumn("region", lit(region))

def calculate_sales_metrics(df):
    """Adds sales-related metrics to the DataFrame."""
    # Define schema for JSON parsing
    promo_discount_schema = StructType([
        StructField("CurrencyCode", StringType(), True),
        StructField("Amount", StringType(), True)  # Convert to double later
    ])
    return (
        df.withColumn("total_sales", col("QuantityOrdered") * col("ItemPrice"))
          .withColumn("PromotionDiscountAmount",
                      from_json(col("PromotionDiscount"), promo_discount_schema)
                      .getItem("Amount")
                      .cast(DoubleType()))
          .withColumn("net_sale", col("total_sales") - col("PromotionDiscountAmount"))
          .dropDuplicates(["OrderId"])
          .filter(col("net_sale") > 0)
    )

# Database Operations
def save_to_sqlite(df, db_name, table_name):
    """Saves a PySpark DataFrame to SQLite."""
    df.toPandas().to_sql(table_name, sqlite3.connect(db_name), if_exists='replace', index=False)

def execute_query(query, db_name=DB_NAME):
    """Executes a SQL query on the SQLite database and returns the results."""
    with sqlite3.connect(db_name) as conn:
        return conn.execute(query).fetchall()

# Main Processing Logic
def main():
    # Initialize Spark
    spark = initialize_spark()

    # Extract and transform data
    data_frames = [
        read_and_tag_data(spark, FILE_PATHS[region], region)
        for region in FILE_PATHS
    ]
    combined_data = calculate_sales_metrics(data_frames[0].union(data_frames[1]))

    # Load data to SQLite
    save_to_sqlite(combined_data, DB_NAME, TABLE_NAME)

    # Validation Queries
    queries = {
        "Total Records": "SELECT COUNT(*) FROM sales_data",
        "Total Sales by Region": "SELECT region, SUM(net_sale) AS total_sales FROM sales_data GROUP BY region",
        "Average Sales per Transaction": "SELECT AVG(net_sale) AS avg_sales FROM sales_data",
        "Duplicate Order IDs": "SELECT OrderId, COUNT(*) FROM sales_data GROUP BY OrderId HAVING COUNT(*) > 1"
    }

    # Print validation results
    for description, query in queries.items():
        print(f"{description}: {execute_query(query)}")

if __name__ == "__main__":
    main()

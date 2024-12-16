# Order Data ETL Script

## Setup Instructions

### 1. Install the required packages:
`pip install pyspark pandas sqlite3`

### 2. Prepare data folder:
Create a folder named "data" in the same directory as the script. Place the sales data files (order_region_a.csv, order_region_b.csv) in this folder.

### 3. Run the script:
`python order_data_etl.py`
This will process the data, transform it, and store the results in an SQLite database (sales_data.db).

Verify the SQLite database (sales_data.db) using any SQLite viewer or by viewing the results of the SQL queries run at the end of the script.

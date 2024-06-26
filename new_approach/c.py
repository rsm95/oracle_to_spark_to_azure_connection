import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Define the paths to the JAR files
jdbc_jars = [
    r"C:\Users\rahul\Desktop\project1234\ojdbc6-11.jar",
    r"C:\Users\rahul\Desktop\project1234\postgresql-42.7.3.jar",
    r"C:\Users\rahul\Desktop\project1234\hadoop-azure-3.4.0.jar",
    r"C:\Users\rahul\Desktop\project1234\azure-storage-8.6.6.jar"
]

# Join the paths with commas
jdbc_jars_str = ",".join(jdbc_jars)

# Initialize Spark session with required JARs
spark = SparkSession.builder \
    .appName("WriteToAzureBlobStorage") \
    .config("spark.jars", jdbc_jars_str) \
    .master("local[*]") \
    .getOrCreate()

# Oracle connection parameters
host = "localhost"
port = "1521"
schema = "XE"
URL = f"jdbc:oracle:thin:@{host}:{port}:{schema}"

print("TABLE")
query = "SELECT * FROM DATAFILE30"

# Read data from Oracle database
df = (spark.read.format("jdbc")
    .option("url", URL)
    .option("query", query)
    .option("user", "sys as sysdba")
    .option("password", "1995")
    .option("driver", "oracle.jdbc.driver.OracleDriver")
    .load()
)

df.printSchema()
df.show()
print(f"Total number of rows of dataset are :-", df.count())

# Azure Blob Storage configuration
storage_account_name = "practice95"
container_name = "dataset"
storage_account_key = "eyGvrinmFZ9/GsfhQxQkZRftMdi3A/2Xvp7PEK+B8NgiIXessYMTU6R4RU52F6hur8SHV1+TVDHr+ASt5T86YA=="

spark.conf.set("fs.azure.account.key.{}.blob.core.windows.net".format(storage_account_name), storage_account_key)

# Define the Azure Blob Storage URL
blob_url = "wasbs://{}@{}.blob.core.windows.net/{}".format(container_name, storage_account_name, "DATAFILE30")

# Write DataFrame directly to Azure Blob Storage
df.write \
    .mode("overwrite") \
    .format("parquet") \
    .save(blob_url)

print("Data written to Azure Blob Storage successfully.")

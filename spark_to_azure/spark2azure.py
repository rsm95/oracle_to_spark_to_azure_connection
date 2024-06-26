import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

#Jar paths
jdbc_jars = [
    r"C:\Users\rahul\Desktop\project1234\ojdbc6-11.jar",
    r"C:\Users\rahul\Desktop\project1234\postgresql-42.7.3.jar"
]

# Join the paths with commas
jdbc_jars_str = ",".join(jdbc_jars)



# Initialize Spark session

spark = SparkSession.builder \
    .appName("WriteToAzureBlobStorage") \
    .config("spark.jars", jdbc_jars_str) \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.2.1,org.apache.hadoop:hadoop-azure-datalake:3.2.1") \
    .master("local[*]") \
    .getOrCreate()

# Oracle connection parameters
host = "localhost"
port = "1521"
schema = "XE"
URL = f"jdbc:oracle:thin:@{host}:{port}:{schema}"

# Azure Blob Storage configurations
storage_account_name = "practice95"
container_name = "dataset2"
storage_account_key = "eyGvrinmFZ9/GsfhQxQkZRftMdi3A/2Xvp7PEK+B8NgiIXessYMTU6R4RU52F6hur8SHV1+TVDHr+ASt5T86YA=="

# Azure Blob Storage configuration in Spark session
spark.conf.set("fs.azure.account.key.{}.dfs.core.windows.net".format(storage_account_name), storage_account_key)

# Loading data from Oracle into DataFrame
query = "SELECT * FROM DATAFILE30"
df = spark.read.format("jdbc") \
    .option("url", URL) \
    .option("query", query) \
    .option("user", "sys as sysdba") \
    .option("password", "1995") \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .load()


# Write DataFrame to Azure Blob Storage function
def write_to_azure_blob_storage(df, blob_url):
    df.write \
        .mode("overwrite") \
        .option("header", "true") \
        .format("csv") \
        .save(blob_url)


# Define Azure Blob Storage URL
blob_base_url = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net"


# Function to process each DataFrame and write to Azure Blob Storage
def process_and_write(df, table_name):
    df.printSchema()
    df.show()
    print(f"Total number of rows of {table_name} are: {df.count()}")

    blob_url = f"{blob_base_url}/{table_name}"
    write_to_azure_blob_storage(df, blob_url)
    print(f"{table_name} written to Azure Blob Storage successfully.")


print("##########################################################")

process_and_write(df,"dataset")

print("##########################################################")

process_and_write(
    df.select(df.ID,df.KEY1.alias("BGIITerritory"), df.KEY2.alias("BGIICoverage"),
              df.KEY3.alias("BGIISymbol"), df.FACTOR.alias("FactorBGII")),
    "table1_BGIILC"
)

print("##########################################################")

process_and_write(
    df.filter(col('TABLE_NUMBER').like('%B.(1)(LC)%')).select(df.ID,df.FACTOR.alias("FactorB1LC")),
    "table2_B1LC"
)

print("##########################################################")

process_and_write(
    df.filter(col('TABLE_NUMBER').like('%C.(2)(LC)%')).select(df.ID,df.KEY1.alias("Coverage"), df.FACTOR.alias("FactorC2LC")),
    "table3_C2LC"
)

print("##########################################################")

process_and_write(
    df.filter(col('TABLE_NUMBER').like('%85.(LC)%')).select(df.ID,df.KEY1.alias("Class_code"), df.KEY2.alias("Coverage85LC"),
                                                            df.KEY3.alias("Symbo85LC"),
                                                            df.KEY4.alias("Constuction_Code"),
                                                            df.FACTOR.alias("FactorBGII")),
    "table4_85LC"
)

print("##########################################################")

process_and_write(
    df.filter(col('TABLE_NUMBER').like('%85.TerrMult(LC)%')).select(df.ID,df.KEY1.alias("Territory"),
                                                                    df.FACTOR.alias("Factor85TERR")),
    "table5_85TerrMultLC"
)

print("##########################################################")
spark.stop()

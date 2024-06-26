import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Define the paths to the JAR files
jdbc_jars = [
    r"C:\Users\rahul\Desktop\project1234\ojdbc6-11.jar",
    r"C:\Users\rahul\Desktop\project1234\postgresql-42.7.3.jar"
]

# Join the paths with commas
jdbc_jars_str = ",".join(jdbc_jars)

# Initialize Spark session with required JARs
spark = SparkSession.builder \
    .appName("WriteToAzurePostgres") \
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
print(f"Total number of rows of dataset are :-",df.count())

# Azure PostgreSQL connection parameters
host = "rsm95.postgres.database.azure.com"
port = "5432"
database = "postgres"
user = "rahul"
password = "Jjm1234#"
sslmode = "allow"  # Using "disable" for SSL

jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}?user={user}&password={password}&sslmode={sslmode}"

# Define PostgreSQL driver
postgres_driver = "org.postgresql.Driver"

# Write DataFrame to Azure PostgreSQL
table_name = "datafile_combined"  # Replace with your actual table name

df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", table_name) \
    .option("driver", postgres_driver) \
    .mode("overwrite") \
    .save()

print("Data written to Azure PostgreSQL successfully.")

print("############ TABLE1 ###################")

df2 = df.filter(col("table_number") == 'BGII(LC)')

df2 = df2.select(df.ID,df.KEY1.alias("BGIITerritory"), df.KEY2.alias("BGIICoverage"), \
                df.KEY3.alias("BGIISymbol"), df.FACTOR.alias("FactorBGII"))

df2.printSchema()
df2.show()
print(f"Total number of rows of table 1 are :-",df2.count())

df2.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "table1") \
    .option("driver", postgres_driver) \
    .mode("overwrite") \
    .save()

print("table1_BGIILC written to Azure PostgreSQL successfully.")


print("############ TABLE2 ###################")

df3 = df.filter(col('TABLE_NUMBER').like('%B.(1)(LC)%'))
df3 = df3.select(df.ID,df.FACTOR.alias("FactorB1LC"))
df3.printSchema()
df3.show()
print(f"Total number of rows of table 2 are :-",df3.count())

df3.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "table2") \
    .option("driver", postgres_driver) \
    .mode("overwrite") \
    .save()

print("table2_B1LC written to Azure PostgreSQL successfully.")


print("############ TABLE3 ###################")

df4 = df.filter(col('TABLE_NUMBER').like('%C.(2)(LC)%'))
df4 = df4.select(df.ID,df.KEY1.alias("Coverage"), df.FACTOR.alias("FactorC2LC"))
df4.printSchema()
df4.show()
print(f"Total number of rows of table 3 are :-",df4.count())

df4.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "table3") \
    .option("driver", postgres_driver) \
    .mode("overwrite") \
    .save()

print("table3_C2LC written to Azure PostgreSQL successfully.")


print("############ TABLE4 ###################")

df5 = df.filter(col('TABLE_NUMBER').like('%85.(LC)%'))
df5 = df5.select(df.ID,df.KEY1.alias("Class_code"), df.KEY2.alias("Coverage85LC"), \
                 df.KEY3.alias("Symbo85LC"), df.KEY4.alias("Constuction_Code"), \
                 df.FACTOR.alias("FactorBGII"))
df5.printSchema()
df5.show()
print(f"Total number of rows of table 4 are :-",df5.count())

df5.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "table4") \
    .option("driver", postgres_driver) \
    .mode("overwrite") \
    .save()


print("table4_85LC written to Azure PostgreSQL successfully.")

print("############ TABLE5 ###################")

df6 = df.filter(col('TABLE_NUMBER').like('%85.TerrMult(LC)%'))
df6 = df6.select(df.ID,df.KEY1.alias("Territory"), df.FACTOR.alias("Factor85TERR"))
df6.printSchema()
df6.show()
print(f"Total number of rows of table 5 are :-",df6.count())

df6.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "table5") \
    .option("driver", postgres_driver) \
    .mode("overwrite") \
    .save()

print("table5_85TerrMultLC written to Azure PostgreSQL successfully.")

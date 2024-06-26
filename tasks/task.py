
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,split,when,lit,rpad,concat,lpad,length
from pyspark.sql.types import StringType

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
    .master("local[*]") \
    .getOrCreate()

# Oracle connection parameters
host = "localhost"
port = "1521"
schema = "XE"
URL = f"jdbc:oracle:thin:@{host}:{port}:{schema}"


# Loading data from Oracle into DataFrame
query = "SELECT * FROM DATAFILE30"
df = spark.read.format("jdbc") \
    .option("url", URL) \
    .option("query", query) \
    .option("user", "sys as sysdba") \
    .option("password", "1995") \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .load()






print("##########################################################")


df2 = df.filter(col('TABLE_NUMBER').like('%85.(LC)%')).select(df.ID,df.KEY1.alias("Class_code"), df.KEY2.alias("Coverage85LC"),
                                                            df.KEY3.alias("Symbo85LC"),
                                                            df.KEY4.alias("Constuction_Code"))

df2.show(101)


print("##########################################################")

df3 = df2.withColumn("Constuction_Code", split(col("Constuction_Code"), r"[\s]*or[\s]*|[\s]*&[\s]*")) \
       .withColumn("Constuction_Code1", col("Constuction_Code").getItem(0)) \
       .withColumn("Constuction_Code2", col("Constuction_Code").getItem(1))

df3 = df3.drop("Constuction_Code")
df3.show(100)

max_length = df3.select(length(col("Class_code")).alias("max_length")).agg({"max_length": "max"}).collect()[0]["max_length"]

print(f"Maximum length of values in 'Class_code' column: {max_length}")



# max_length = df3.select(length(col("Class_code")).alias("max_length")).agg({"max_length": "max"}).collect()[0]["max_length"]
# print("####:-",max_length)
# df4 = df3.withColumn("Class_code", lpad(col("Class_code").cast(StringType()), max_length, '0'))
#
#
# df4.show(1000)
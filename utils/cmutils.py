import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


class DatabaseOperations:
    def __init__(self):
        self.spark = self._init_spark()
        self.oracle_url = self._get_oracle_url()
        self.pg_url = self._get_postgres_url()

    def _init_spark(self):
        jdbc_jars = [
            r"C:\Users\rahul\Desktop\project1234\ojdbc6-11.jar",
            r"C:\Users\rahul\Desktop\project1234\postgresql-42.7.3.jar"
        ]
        jdbc_jars_str = ",".join(jdbc_jars)

        return SparkSession.builder \
            .appName("WriteToAzurePostgres") \
            .config("spark.jars", jdbc_jars_str) \
            .master("local[*]") \
            .getOrCreate()

    def _get_oracle_url(self):
        host = os.getenv("ORACLE_HOST", "localhost")
        port = os.getenv("ORACLE_PORT", "1521")
        schema = os.getenv("ORACLE_SCHEMA", "XE")
        user = os.getenv("ORACLE_USER", "sys as sysdba")
        password = os.getenv("ORACLE_PASSWORD", "1995")

        return f"jdbc:oracle:thin:@{host}:{port}:{schema}"

    def _get_postgres_url(self):
        host = os.getenv("PG_HOST", "rsm95.postgres.database.azure.com")
        port = os.getenv("PG_PORT", "5432")
        database = os.getenv("PG_DATABASE", "postgres")
        user = os.getenv("PG_USER", "rahul")
        password = os.getenv("PG_PASSWORD", "Jjm1234#")
        sslmode = os.getenv("PG_SSLMODE", "allow")

        return f"jdbc:postgresql://{host}:{port}/{database}?user={user}&password={password}&sslmode={sslmode}"

    def read_oracle_table(self, query):
        return self.spark.read.format("jdbc") \
            .option("url", self.oracle_url) \
            .option("query", query) \
            .option("user", os.getenv("ORACLE_USER", "sys as sysdba")) \
            .option("password", os.getenv("ORACLE_PASSWORD", "1995")) \
            .option("driver", "oracle.jdbc.driver.OracleDriver") \
            .load()

    def write_to_postgres(self, df, table_name):
        df.write \
            .format("jdbc") \
            .option("url", self.pg_url) \
            .option("dbtable", table_name) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()

    def filter_and_select(self, df, filter_col, filter_val, select_cols):
        filtered_df = df.filter(col(filter_col).like(filter_val))
        selected_df = filtered_df.select([col(c).alias(a) for c, a in select_cols])
        return selected_df

    def process_table(self, df, filter_col, filter_val, select_cols, table_name):
        filtered_df = self.filter_and_select(df, filter_col, filter_val, select_cols)
        filtered_df.show()
        print(f"Total number of rows of {table_name} are :-", filtered_df.count())
        self.write_to_postgres(filtered_df, table_name)
        print(f"{table_name} written to Azure PostgreSQL successfully.")
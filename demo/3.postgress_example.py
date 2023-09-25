import os
import time

from pyspark.sql import SparkSession

tmp_path = os.path.join(os.getcwd(), "tmp")

spark = (
    SparkSession.builder.master("local[3]")
    .config("spark.local.dir", tmp_path)
    .config("spark.driver.memory", "2G")
    .config("spark.jars", os.path.join(os.getcwd(), "jars", "postgresql-42.6.0.jar"))
    .appName("Postgress Example")
    .getOrCreate()
)

source_file_path = os.path.join(os.getcwd(), "data", "Employee.csv")
jdbc_url = "jdbc:postgresql://localhost:5432/pyspark"


df = spark.read.format("csv").option("header", True).load(source_file_path)
print(f"Partitions: {df.rdd.getNumPartitions()}")
df.show(n=5, truncate=False)


df.write.format("jdbc").mode("overwrite").option("url", jdbc_url).option(
    "driver", "org.postgresql.Driver"
).option("dbtable", "public.employees").option("user", "postgres").option(
    "password", "Postgres@123"
).save()
print("Data has beenn successfully written to the Postgres DB....")


print("================== READ TABLE DATA=================")
seq_db_df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("driver", "org.postgresql.Driver")
    .option("dbtable", "employees")
    .option("user", "postgres")
    .option("password", "Postgres@123")
    .load()
)
seq_db_df.show(n=5, truncate=False)

print("READ DATA USING QUERY")
st = time.perf_counter()
query_db_df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("driver", "org.postgresql.Driver")
    # .option("dbtable", "select firstName, gender, team from employees")
    .option("query", "SELECT * FROM employees")
    .option("user", "postgres")
    .option("password", "Postgres@123")
    .load()
)
et = time.perf_counter()
query_db_df.show(n=5, truncate=False)

spark.stop()

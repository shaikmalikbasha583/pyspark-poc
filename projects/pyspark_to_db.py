import os
import time

from helper.spark_object import close_spark_session_object, get_spark_session_object

spark = get_spark_session_object()

source_file_path = os.path.join(os.getcwd(), "data", "survey-bands-full.csv")
driver_path = os.path.join(os.getcwd(), "sqlite-jdbc-3.7.2")

# spark.conf.set("spark.jars", driver_path)
# spark.conf.set("spark.driver.extraClassPath", driver_path)

url = r"jdbc:sqlite:C:\Users\costrategix\Workspace\python-workspace\apache-spark-poc\database1.sqlite3"

df = spark.read.format("csv").option("header", True).load(source_file_path)

df.show(n=5)

start = time.perf_counter()

print("Inserting...")
df.write.format("jdbc").option("driver", driver_path).option("overwrite", True).option(
    "url", url
).option("dbtable", "bands").save()

time_taken = time.perf_counter() - start
print(f"Done in {round(time_taken, 2)} second(s)")


close_spark_session_object(spark_obj=spark)

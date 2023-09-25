import os
from helper.spark_object import get_spark_session_object, close_spark_session_object

spark = get_spark_session_object()

source_file_path = os.path.join(os.getcwd(), "data", "employees")

df = spark.read.format("csv").option("header", True).load(source_file_path)
print(f"Num of Partitions: {df.rdd.getNumPartitions()}")
df.show(n=5, truncate=False)

close_spark_session_object(spark_obj=spark)

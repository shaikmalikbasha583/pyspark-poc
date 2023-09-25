"""
For parallel processing, Apache Spark uses shared variables. 
A copy of shared variable goes on each node of the cluster 
when the driver sends a task to the executor on the cluster, 
so that it can be used for performing tasks.

There are two types of shared variables supported by Apache Spark

1. Broadcast
2. Accumulator
"""

import pyspark.sql.functions as F
import pyspark.sql.types as T

from helper.spark_object import close_spark_session_object, get_spark_session_object

spark = get_spark_session_object()


data = [
    (1, "Malik", "P", 93),
    (2, "Aiman", "P", 83),
    (3, "Aina", "P", 80),
    (4, "Banu", "F", 30),
    (5, "Shaik", "F", 33),
    (6, "Abdul", "F", 34),
]

df = spark.createDataFrame(data, schema=["id", "name", "status", "marks"])
print(f"PartitionsDF: {df.rdd.getNumPartitions()}")
df.show(truncate=False)
"""
PartitionsDF: 3
+---+-----+------+-----+
|id |name |status|marks|
+---+-----+------+-----+
|1  |Malik|P     |93   |
|2  |Aiman|P     |83   |
|3  |Aina |P     |80   |
|4  |Banu |F     |30   |
|5  |Shaik|F     |33   |
|6  |Abdul|F     |34   |
+---+-----+------+-----+
"""

map_ = {"P": "Passed", "F": "Failed"}
# creating broadcast variable and storing the information in broadcast_status variable
broadcast_status = spark.sparkContext.broadcast(map_)


def gen_abbrevation_by_key(key: str) -> str:
    return broadcast_status.value[key]


gen_abbrevation_by_key_udf = F.udf(gen_abbrevation_by_key, returnType=T.StringType())

new_df = df.withColumn("full_status", gen_abbrevation_by_key_udf("status"))
print(f"NewPartitionsDF: {new_df.rdd.getNumPartitions()}")
new_df.show(truncate=False)
"""
NewPartitionsDF: 3
+---+-----+------+-----+-----------+
|id |name |status|marks|full_status|
+---+-----+------+-----+-----------+
|1  |Malik|P     |93   |Passed     |
|2  |Aiman|P     |83   |Passed     |
|3  |Aina |P     |80   |Passed     |
|4  |Banu |F     |30   |Failed     |
|5  |Shaik|F     |33   |Failed     |
|6  |Abdul|F     |34   |Failed     |
+---+-----+------+-----+-----------+
"""

## Close the session
close_spark_session_object(spark_obj=spark)

import os

import pyspark.sql.types as T

from helper.spark_object import close_spark_session_object, get_spark_session_object

spark = get_spark_session_object()
source_files_dir = os.path.join(os.getcwd(), "data", "json-data")
input_schema = T.StructType(
    [
        T.StructField("RecordNumber", T.IntegerType(), True),
        T.StructField("Zipcode", T.StringType(), True),
        T.StructField("ZipCodeType", T.StringType(), True),
        T.StructField("City", T.StringType(), True),
        T.StructField("State", T.StringType(), True),
        T.StructField("LocationType", T.StringType(), True),
        T.StructField("Lat", T.StringType(), True),
        T.StructField("Long", T.StringType(), True),
        T.StructField("Xaxis", T.StringType(), True),
        T.StructField("Yaxis", T.StringType(), True),
        T.StructField("Zaxis", T.StringType(), True),
        T.StructField("WorldRegion", T.StringType(), True),
        T.StructField("Country", T.StringType(), True),
        T.StructField("LocationText", T.StringType(), True),
        T.StructField("Location", T.StringType(), True),
        T.StructField("Decommisioned", T.StringType(), True),
    ]
)

df = spark.read.schema(input_schema).json(source_files_dir)
df.printSchema()

df.select("Zipcode").groupBy("Zipcode").count().show(truncate=False)
"""
+-------+-----+
|Zipcode|count|
+-------+-----+
|32564  |1    |
|85210  |1    |
|36275  |2    |
|35146  |2    |
|35585  |2    |
|32046  |1    |
|27203  |3    |
|34445  |1    |
|27007  |3    |
|27204  |3    |
|85209  |1    |
|34487  |1    |
|76166  |1    |
|null   |72   |
|709    |2    |
|704    |3    |
|76177  |2    |
+-------+-----+
"""

## Closing the SparkSession Object
close_spark_session_object(spark_obj=spark)

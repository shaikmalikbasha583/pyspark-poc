"""
## Creation of Empty RDD in PySpark

"""
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from helper.spark_object import close_spark_session_object, get_spark_session_object

spark = get_spark_session_object(app_name="PySpark Empty DataFrame")

emptyRDD = spark.sparkContext.emptyRDD()
print(f"Empty DataFrame with RDD: {emptyRDD}")
"""Empty DataFrame with RDD: EmptyRDD[0] at emptyRDD at NativeMethodAccessorImpl.java:0"""

# Creates Empty RDD using parallelize
emptyRDD = spark.sparkContext.parallelize([])
print(f"Empty DataFrame with RDD Parallelize: {emptyRDD}")
"""Empty DataFrame with RDD Parallelize: ParallelCollectionRDD[1] at readRDDFromFile at PythonRDD.scala:287"""


# Creates Empty DataFrame with Schema
schema = StructType(
    [
        StructField("id", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("is_active", BooleanType(), nullable=True),
    ]
)

df = spark.createDataFrame([], schema=schema)
df.printSchema()
"""
root
 |-- id: integer (nullable = false)
 |-- name: string (nullable = true)
 |-- is_active: boolean (nullable = true)
"""
print(f"Empty DataFrame: {df}")
"""Empty DataFrame: DataFrame[id: int, name: string, is_active: boolean]"""

## Closing the spark session object
close_spark_session_object(spark_obj=spark)

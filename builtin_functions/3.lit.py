import pyspark.sql.functions as F
import pyspark.sql.types as T

from helper.spark_object import close_spark_session_object, get_spark_session_object

spark = get_spark_session_object()


student_data = [
    ("Malik", 26, 95),
    ("Aiman", 26, 85),
    ("Aina", 26, 34),
    ("Deepika", 26, 25),
]

my_schema = T.StructType(
    [
        T.StructField("name", T.StringType(), nullable=False),
        T.StructField("age", T.IntegerType(), nullable=False),
        T.StructField("score", T.IntegerType(), nullable=False),
    ]
)

df = spark.createDataFrame(data=student_data, schema=my_schema)
df.printSchema()
"""
root
 |-- name: string (nullable = false)
 |-- age: integer (nullable = false)
 |-- score: integer (nullable = false)
"""
df.show(truncate=False)
"""
+-------+---+-----+
|name   |age|score|
+-------+---+-----+
|Malik  |26 |95   |
|Aiman  |26 |85   |
|Aina   |26 |34   |
|Deepika|26 |25   |
+-------+---+-----+
"""


df2 = df.select(
    F.col("name"), F.col("age"), F.col("score"), F.lit("BITS").alias("college")
)
df2.show(truncate=False)
"""
+-------+---+-----+-------+
|name   |age|score|college|
+-------+---+-----+-------+
|Malik  |26 |95   |BITS   |
|Aiman  |26 |85   |BITS   |
|Aina   |26 |34   |BITS   |
|Deepika|26 |25   |BITS   |
+-------+---+-----+-------+
"""

df.withColumn("satus", F.when(df["score"] < 35, "FAILED").otherwise("PASSED")).show(
    truncate=False
)
"""
+-------+---+-----+------+
|name   |age|score|satus |
+-------+---+-----+------+
|Malik  |26 |95   |PASSED|
|Aiman  |26 |85   |PASSED|
|Aina   |26 |34   |FAILED|
|Deepika|26 |25   |FAILED|
+-------+---+-----+------+
"""

close_spark_session_object(spark_obj=spark)

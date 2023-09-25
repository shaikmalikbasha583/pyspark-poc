import pyspark.sql.functions as F

from helper.spark_object import close_spark_session_object, get_spark_session_object

spark = get_spark_session_object()

data = (
    ("Java", 4000, 5),
    ("Python", 4600, 10),
    ("Scala", 4100, 15),
    ("Scala", 4500, 15),
    ("PHP", 3000, 20),
)
columns = ["CourseName", "CourseFee", "CourseDiscount"]

df = spark.createDataFrame(data=data, schema=columns)
df.show(truncate=False)
"""
+----------+---------+--------------+
|CourseName|CourseFee|CourseDiscount|
+----------+---------+--------------+
|Java      |4000     |5             |
|Python    |4600     |10            |
|Scala     |4100     |15            |
|Scala     |4500     |15            |
|PHP       |3000     |20            |
+----------+---------+--------------+
"""


## Custom transformation 1
def to_upper_str_columns(df):
    return df.withColumn("CourseName", F.upper(df.CourseName))


## Custom transformation 2
def reduce_price(df, reduceBy):
    return df.withColumn("NewFee", df.CourseFee - reduceBy)


## Custom transformation 3
def apply_discount(df):
    return df.withColumn(
        "DiscountedFee", df.NewFee - (df.NewFee * df.CourseDiscount) / 100
    )


## Custom transformation 4
def select_columns(df, cols):
    return df.select(cols)


df.transform(to_upper_str_columns).transform(reduce_price, 1000).transform(
    apply_discount
).transform(select_columns, ["CourseName", "DiscountedFee"]).show(truncate=False)
""""
+----------+-------------+
|CourseName|DiscountedFee|
+----------+-------------+
|JAVA      |2850.0       |
|PYTHON    |3240.0       |
|SCALA     |2635.0       |
|SCALA     |2975.0       |
|PHP       |1600.0       |
+----------+-------------+
"""

## Close the spark session
close_spark_session_object(spark_obj=spark)

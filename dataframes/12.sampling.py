from helper.spark_object import close_spark_session_object, get_spark_session_object

spark = get_spark_session_object()

df = spark.range(100)

print(df.sample(0.1, 123).collect())
"""
Output: 36,37,41,43,56,66,69,75,83
"""

print(df.sample(0.1, 123).collect())
"""
Output: 36,37,41,43,56,66,69,75,83
"""

print(df.sample(0.1, 456).collect())
"""
Output: 19,21,42,48,49,50,75,80
"""

close_spark_session_object(spark_obj=spark)

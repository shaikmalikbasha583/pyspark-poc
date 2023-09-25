from helper.spark_object import close_spark_session_object, get_spark_session_object

spark = get_spark_session_object()

rdd1 = spark.sparkContext.parallelize((("a", 55), ("b", 56), ("c", 57)))
rdd2 = spark.sparkContext.parallelize((("a", 60), ("b", 65), ("d", 61)))
joinrdd = rdd1.join(rdd2).collect()

print(joinrdd)
"""
[('b', (56, 65)), ('a', (55, 60))]
"""

close_spark_session_object(spark_obj=spark)

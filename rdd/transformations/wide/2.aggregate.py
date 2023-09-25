from helper.spark_object import close_spark_session_object, get_spark_session_object

spark = get_spark_session_object()

rdd = spark.sparkContext.parallelize([("a", 1), ("b", 1), ("a", 1)])
sorted(rdd.groupByKey().mapValues(len).collect())
"""
[('a', 2), ('b', 1)]
"""

sorted(rdd.groupByKey().mapValues(list).collect())
"""
[('a', [1, 1]), ('b', [1])]
"""


close_spark_session_object(spark_obj=spark)

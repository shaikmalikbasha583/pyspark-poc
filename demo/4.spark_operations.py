from helper.spark_object import close_spark_session_object, get_spark_session_object

spark = get_spark_session_object()


rdd = spark.sparkContext.parallelize(range(1, 13))
print(f"Partitions: {rdd.getNumPartitions()}")

## 1. Map

close_spark_session_object(spark_obj=spark)

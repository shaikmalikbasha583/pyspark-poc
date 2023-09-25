from helper.spark_object import close_spark_session_object, get_spark_session_object

spark = get_spark_session_object()

## parallelize()
numRDD = spark.sparkContext.parallelize(range(10), numSlices=6)
print(f"\nPartitions: {numRDD.getNumPartitions()}")
"""
Partitions: 6
"""


## textFile() method
fileRDD = spark.sparkContext.textFile("data/names.txt", minPartitions=6)
print(f"\nPartitions: {numRDD.getNumPartitions()}")
"""
Partitions: 6
"""

close_spark_session_object(spark_obj=spark)

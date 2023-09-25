import os

from helper.spark_object import close_spark_session_object, get_spark_session_object

spark = get_spark_session_object()

source_file_path = os.path.join(os.getcwd(), "data", "names.txt")

# Create spark session with local[3]
rdd = spark.sparkContext.parallelize(range(0, 20))
print(f"From local[3] : {rdd.getNumPartitions()}")

# Use parallelize with 6 partitions
rdd1 = spark.sparkContext.parallelize(range(0, 26), 6)
print(f"Parallelize in 6 slices: {rdd1.getNumPartitions()}")

rddFromFile = spark.sparkContext.textFile(source_file_path, 10)
print(f"TextFile Partitions: {rddFromFile.getNumPartitions()}")

print("=====================WRITING=====================")
rdd1.saveAsTextFile("sink/rdd/tmp/partition")

print("Repartitioning the rdd1 in 4 slices...")
rdd2 = rdd1.repartition(4)
print(f"Parallelize in 4 slices: {rdd1.getNumPartitions()}")
rdd2.saveAsTextFile("sink/rdd/tmp/repartition")


# Using coalesce()
rdd3 = rdd1.coalesce(4)
print(f"coalesce - Repartition size: {rdd3.getNumPartitions()}")
rdd3.saveAsTextFile("sink/rdd/tmp/coalesce")

## Close the session Object
close_spark_session_object(spark_obj=spark)

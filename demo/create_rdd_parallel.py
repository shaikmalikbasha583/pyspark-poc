import os
import shutil

from helper.spark_object import close_spark_session_object, get_spark_session_object

spark = get_spark_session_object()
par_dir = "sink/rdd/parallel"
text_dir = "sink/rdd/text"

if os.path.isdir(par_dir) or os.path.exists(par_dir):
    shutil.rmtree(par_dir)
if os.path.isdir(text_dir) or os.path.exists(text_dir):
    shutil.rmtree(text_dir)

## parallelize()
numRDD = spark.sparkContext.parallelize(range(10), numSlices=6)
print(f"\nPartitions: {numRDD.getNumPartitions()}")
numRDD.saveAsTextFile(par_dir)
"""
Partitions: 6
"""


## textFile() method
fileRDD = spark.sparkContext.textFile("data/names.txt", minPartitions=6)
print(f"\nPartitions: {numRDD.getNumPartitions()}")
fileRDD.saveAsTextFile(text_dir)
"""
Partitions: 6
"""

close_spark_session_object(spark_obj=spark)

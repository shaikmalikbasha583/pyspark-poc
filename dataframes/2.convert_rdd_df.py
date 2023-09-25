"""
Convert RDD to DataFrame
"""

from helper.spark_object import close_spark_session_object, get_spark_session_object

spark = get_spark_session_object(app_name="PySpark RDD-DataFrame")

users = [
    {"id": 1, "name": "Shaik", "age": 20, "gender": "F", "is_active": True},
    {"id": 2, "name": "Malik", "age": 26, "gender": "M", "is_active": True},
    {"id": 3, "name": "Abdul", "age": 23, "gender": "M", "is_active": True},
    {"id": 4, "name": "Basha", "age": 24, "gender": "M", "is_active": False},
    {"id": 5, "name": "Aiman", "age": 18, "gender": "F", "is_active": True},
]

rdd = spark.sparkContext.parallelize(users)
print(f"RDD: Partitions: {rdd.getNumPartitions()}")
print(rdd.collect())

df = rdd.toDF()
df.printSchema()
print(f"DF: Partitions: {df.rdd.getNumPartitions()}")
df.show()

## Closing the spark session object
close_spark_session_object(spark_obj=spark)

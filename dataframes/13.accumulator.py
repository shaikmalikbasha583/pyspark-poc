"""
For parallel processing, Apache Spark uses shared variables. 
A copy of shared variable goes on each node of the cluster 
when the driver sends a task to the executor on the cluster, 
so that it can be used for performing tasks.

There are two types of shared variables supported by Apache Spark

1. Broadcast
2. Accumulator
"""

from helper.spark_object import close_spark_session_object, get_spark_session_object

spark = get_spark_session_object()

game_data = [
    ("USA", "Germany", 9),
    ("Germany", "Italy", 5),
    ("Italy", "India", 2),
    ("India", "Italy", 4),
    ("France", "USA", 1),
    ("Italy", "India", 2),
    ("USA", "France", 3),
    ("India", "France", 7),
]

df = spark.createDataFrame(
    data=game_data, schema=["homeCountry", "hostCountry", "matches"]
)
print(f"Partitions: {df.rdd.getNumPartitions()}")
df.show(truncate=False)


matchcounter = spark.sparkContext.accumulator(0)
print(f"InitialValue of accumulator: {matchcounter}")


def counterfn(match_num):
    home = match_num["homeCountry"]
    host = match_num["hostCountry"]
    if home == "India" or host == "India":
        # print(f"{match_num}:- Adding {match_num['matches']} to {matchcounter}")
        matchcounter.add(match_num["matches"])


df.foreach(lambda match: counterfn(match))
print(f"FinalValue of Accumulator: {matchcounter}")

print("=========PARTITIONS===========")

df = df.coalesce(4)
print(f"Coalesce: {df.rdd.getNumPartitions()}")
df.show()

matchcounter = spark.sparkContext.accumulator(0)
print(f"InitialValue of accumulator: {matchcounter}")

df.foreach(lambda match: counterfn(match))
print(f"FinalValue of Accumulator: {matchcounter}")

close_spark_session_object(spark_obj=spark)

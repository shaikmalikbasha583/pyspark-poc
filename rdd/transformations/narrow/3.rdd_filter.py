from helper.spark_object import close_spark_session_object, get_spark_session_object

spark = get_spark_session_object()


users = [
    {
        "id": 1,
        "name": {"first_name": "Shaik", "last_name": "Fiza"},
        "age": 20,
        "gender": "F",
    },
    {
        "id": 2,
        "name": {"first_name": "Shaik", "last_name": "Malik"},
        "age": 25,
        "gender": "M",
    },
    {
        "id": 3,
        "name": {"first_name": "Shaik", "last_name": "Abdul"},
        "age": 25,
        "gender": "M",
    },
    {
        "id": 4,
        "name": {"first_name": "Shaik", "last_name": "Banu"},
        "age": 18,
        "gender": "F",
    },
    {
        "id": 5,
        "name": {"first_name": "Shaik", "last_name": "Aiman"},
        "age": 19,
        "gender": "F",
    },
]

rdd = spark.sparkContext.parallelize(users)
print(f"RDD Partitions: {rdd.getNumPartitions()}")
print(rdd.collect())
"""
RDD Partitions: 3
[
    {'id': 1, 'name': {'first_name': 'Shaik', 'last_name': 'Fiza'}, 'age': 20, 'gender': 'F'}, 
    {'id': 2, 'name': {'first_name': 'Shaik', 'last_name': 'Malik'}, 'age': 25, 'gender': 'M'}, 
    {'id': 3, 'name': {'first_name': 'Shaik', 'last_name': 'Abdul'}, 'age': 25, 'gender': 'M'}, 
    {'id': 4, 'name': {'first_name': 'Shaik', 'last_name': 'Banu'}, 'age': 18, 'gender': 'F'}, 
    {'id': 5, 'name': {'first_name': 'Shaik', 'last_name': 'Aiman'}, 'age': 19, 'gender': 'F'}
]
"""

rdd.filter(lambda user: user["gender"] == "M").foreach(lambda user: print(user))
"""
{'id': 2, 'name': {'first_name': 'Shaik', 'last_name': 'Malik'}, 'age': 25, 'gender': 'M'}
{'id': 3, 'name': {'first_name': 'Shaik', 'last_name': 'Abdul'}, 'age': 25, 'gender': 'M'}
"""

close_spark_session_object(spark_obj=spark)

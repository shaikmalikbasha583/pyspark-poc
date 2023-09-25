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

data = rdd.flatMap(
    lambda x: (
        x["id"],
        x["name"]["first_name"] + " " + x["name"]["last_name"],
        x["age"],
        x["gender"],
    )
).collect()
print(f"flatMap Data: {data}")
"""
flatMap Data: [1, 'Shaik Fiza', 20, 'F', 
2, 'Shaik Malik', 25, 'M', 
3, 'Shaik Abdul', 25, 'M', 
4, 'Shaik Banu', 18, 'F', 
5, 'Shaik Aiman', 19, 'F']
"""

df = rdd.flatMap(
    lambda x: [
        (
            x["id"],
            x["name"]["first_name"] + " " + x["name"]["last_name"],
            x["age"],
            x["gender"],
        )
    ]
).toDF(schema=["id", "full_name", "age", "gender"])
print(f"DataFrame Partitions: {df.rdd.getNumPartitions()}")
df.show(truncate=False)
"""
DataFrame Partitions: 3
+---+-----------+---+------+
|id |full_name  |age|gender|
+---+-----------+---+------+
|1  |Shaik Fiza |20 |F     |
|2  |Shaik Malik|25 |M     |
|3  |Shaik Abdul|25 |M     |
|4  |Shaik Banu |18 |F     |
|5  |Shaik Aiman|19 |F     |
+---+-----------+---+------+
"""

close_spark_session_object(spark_obj=spark)

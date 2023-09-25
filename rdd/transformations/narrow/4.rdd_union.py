from helper.spark_object import close_spark_session_object, get_spark_session_object

spark = get_spark_session_object()

females = [
    {
        "id": 1,
        "name": {"first_name": "Shaik", "last_name": "Fiza"},
        "age": 20,
        "gender": "F",
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
males = [
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
]
males_rdd = spark.sparkContext.parallelize(males)
females_rdd = spark.sparkContext.parallelize(females)

joined_users = males_rdd.union(females_rdd)

joined_users.foreach(
    lambda x: print(
        f"I am {x['name']['first_name']} {x['name']['last_name']}. I am {x['age']} year(s) old."
    )
)
"""
I am Shaik Malik. I am 25 year(s) old.
I am Shaik Abdul. I am 25 year(s) old.
I am Shaik Fiza. I am 20 year(s) old.
I am Shaik Banu. I am 18 year(s) old.
I am Shaik Aiman. I am 19 year(s) old.
"""

close_spark_session_object(spark_obj=spark)

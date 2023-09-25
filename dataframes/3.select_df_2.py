import os

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from helper.spark_object import close_spark_session_object, get_spark_session_object

spark = get_spark_session_object()

source_file_path = os.path.join(os.getcwd(), "data", "Movies.csv")

df = spark.read.format("csv").option("header", True).load(source_file_path)

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

schema = StructType(
    [
        StructField("id", IntegerType(), nullable=False),
        StructField(
            "name",
            StructType(
                [
                    StructField("first_name", StringType(), nullable=False),
                    StructField("last_name", StringType(), nullable=False),
                ]
            ),
        ),
        StructField("age", IntegerType(), nullable=False),
        StructField("gender", StringType(), nullable=False),
    ]
)

df = spark.createDataFrame(users, schema=schema)
n = 5


df.show(n=n, truncate=False)
"""
+---+--------------+---+------+
|id |name          |age|gender|
+---+--------------+---+------+
|1  |{Shaik, Fiza} |20 |F     |
|2  |{Shaik, Malik}|25 |M     |
|3  |{Shaik, Abdul}|25 |M     |
|4  |{Shaik, Banu} |18 |F     |
|5  |{Shaik, Aiman}|19 |F     |
+---+--------------+---+------+
"""

df.select("name").show(n=n, truncate=False)
"""
+--------------+
|name          |
+--------------+
|{Shaik, Fiza} |
|{Shaik, Malik}|
|{Shaik, Abdul}|
|{Shaik, Banu} |
|{Shaik, Aiman}|
+--------------+
"""

df.select("name.first_name", "name.last_name").show(n=n, truncate=False)
"""
+----------+---------+
|first_name|last_name|
+----------+---------+
|     Shaik|     Fiza|
|     Shaik|    Malik|
|     Shaik|    Abdul|
|     Shaik|     Banu|
|     Shaik|    Aiman|
+----------+---------+
"""

df.select("name.*", "id", "age", "gender").show(n=n, truncate=False)
"""
+----------+---------+---+---+------+
|first_name|last_name|id |age|gender|
+----------+---------+---+---+------+
|Shaik     |Fiza     |1  |20 |F     |
|Shaik     |Malik    |2  |25 |M     |
|Shaik     |Abdul    |3  |25 |M     |
|Shaik     |Banu     |4  |18 |F     |
|Shaik     |Aiman    |5  |19 |F     |
+----------+---------+---+---+------+
"""


### Close the session Object
close_spark_session_object(spark_obj=spark)

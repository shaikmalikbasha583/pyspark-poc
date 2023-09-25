from helper.spark_object import get_spark_session_object, close_spark_session_object


spark = get_spark_session_object()


simpleData = [
    ("James", "Sales", "NY", 90000, 34, 10000),
    ("Michael", "Sales", "NY", 86000, 56, 20000),
    ("Robert", "Sales", "CA", 81000, 30, 23000),
    ("Maria", "Finance", "CA", 90000, 24, 23000),
    ("Raman", "Finance", "CA", 99000, 40, 24000),
    ("Scott", "Finance", "NY", 83000, 36, 19000),
    ("Jen", "Finance", "NY", 79000, 53, 15000),
    ("Jeff", "Marketing", "CA", 80000, 25, 18000),
    ("Kumar", "Marketing", "NY", 91000, 50, 21000),
]

schema = ["employeeName", "department", "state", "salary", "age", "bonus"]
df = spark.createDataFrame(data=simpleData, schema=schema)
df.printSchema()
"""
root
 |-- employeeName: string (nullable = true)
 |-- department: string (nullable = true)
 |-- state: string (nullable = true)
 |-- salary: long (nullable = true)
 |-- age: long (nullable = true)
 |-- bonus: long (nullable = true)
"""

df.show(truncate=False)
"""
+------------+----------+-----+------+---+-----+
|employeeName|department|state|salary|age|bonus|
+------------+----------+-----+------+---+-----+
|James       |Sales     |NY   |90000 |34 |10000|
|Michael     |Sales     |NY   |86000 |56 |20000|
|Robert      |Sales     |CA   |81000 |30 |23000|
|Maria       |Finance   |CA   |90000 |24 |23000|
|Raman       |Finance   |CA   |99000 |40 |24000|
|Scott       |Finance   |NY   |83000 |36 |19000|
|Jen         |Finance   |NY   |79000 |53 |15000|
|Jeff        |Marketing |CA   |80000 |25 |18000|
|Kumar       |Marketing |NY   |91000 |50 |21000|
+------------+----------+-----+------+---+-----+
"""

df.groupBy("department").count().show()
"""
+----------+-----+
|department|count|
+----------+-----+
|     Sales|    3|
|   Finance|    4|
| Marketing|    2|
+----------+-----+
"""

df.groupBy("department").sum("salary").show()
"""
+----------+-----------+
|department|sum(salary)|
+----------+-----------+
|     Sales|     257000|
|   Finance|     351000|
| Marketing|     171000|
+----------+-----------+
"""

df.filter(df.age > 30).groupBy(df.department).count().show()

"""
+----------+-----+
|department|count|
+----------+-----+
|     Sales|    2|
|   Finance|    3|
| Marketing|    1|
+----------+-----+
"""

## Closing the session object
close_spark_session_object(spark_obj=spark)

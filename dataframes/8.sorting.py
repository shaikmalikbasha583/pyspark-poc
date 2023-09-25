from pyspark.sql.types import IntegerType

from helper.spark_object import close_spark_session_object, get_spark_session_object

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
columns = ["employeeName", "department", "state", "salary", "age", "bonus"]
df = spark.createDataFrame(data=simpleData, schema=columns)

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

## Convert Age type to int
df = df.withColumn("age", df.age.cast(IntegerType()))


df.sort("employeeName", "salary").show(truncate=False)
"""
+------------+----------+-----+------+---+-----+
|employeeName|department|state|salary|age|bonus|
+------------+----------+-----+------+---+-----+
|James       |Sales     |NY   |90000 |34 |10000|
|Jeff        |Marketing |CA   |80000 |25 |18000|
|Jen         |Finance   |NY   |79000 |53 |15000|
|Kumar       |Marketing |NY   |91000 |50 |21000|
|Maria       |Finance   |CA   |90000 |24 |23000|
|Michael     |Sales     |NY   |86000 |56 |20000|
|Raman       |Finance   |CA   |99000 |40 |24000|
|Robert      |Sales     |CA   |81000 |30 |23000|
|Scott       |Finance   |NY   |83000 |36 |19000|
+------------+----------+-----+------+---+-----+
"""

# df.sort(df.employeeName.asc(), df.age.desc()).show()
df.orderBy(df.employeeName.asc(), df.age.desc()).show()
"""
+------------+----------+-----+------+---+-----+
|employeeName|department|state|salary|age|bonus|
+------------+----------+-----+------+---+-----+
|       James|     Sales|   NY| 90000| 34|10000|
|        Jeff| Marketing|   CA| 80000| 25|18000|
|         Jen|   Finance|   NY| 79000| 53|15000|
|       Kumar| Marketing|   NY| 91000| 50|21000|
|       Maria|   Finance|   CA| 90000| 24|23000|
|     Michael|     Sales|   NY| 86000| 56|20000|
|       Raman|   Finance|   CA| 99000| 40|24000|
|      Robert|     Sales|   CA| 81000| 30|23000|
|       Scott|   Finance|   NY| 83000| 36|19000|
+------------+----------+-----+------+---+-----+
"""

print("Sorting with SQL...")
df.createOrReplaceTempView("EMP")
spark.sql(
    """
    SELECT employeeName, department, state, salary, age, bonus 
        FROM EMP 
    ORDER BY department ASC
    """
).show(truncate=False)
"""
Sorting with SQL...
+------------+----------+-----+------+---+-----+
|employeeName|department|state|salary|age|bonus|
+------------+----------+-----+------+---+-----+
|Jen         |Finance   |NY   |79000 |53 |15000|
|Maria       |Finance   |CA   |90000 |24 |23000|
|Raman       |Finance   |CA   |99000 |40 |24000|
|Scott       |Finance   |NY   |83000 |36 |19000|
|Jeff        |Marketing |CA   |80000 |25 |18000|
|Kumar       |Marketing |NY   |91000 |50 |21000|
|James       |Sales     |NY   |90000 |34 |10000|
|Michael     |Sales     |NY   |86000 |56 |20000|
|Robert      |Sales     |CA   |81000 |30 |23000|
+------------+----------+-----+------+---+-----+
"""

## Closing the session
close_spark_session_object(spark_obj=spark)

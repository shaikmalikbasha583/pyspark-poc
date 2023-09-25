from helper.spark_object import close_spark_session_object, get_spark_session_object

spark = get_spark_session_object()

## Prepare Data
data = [
    ("James", "Sales", 3000),
    ("Swathi", "Sales", 4600),
    ("Saif", "Accounts", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Chandresh", "Finance", 3300),
    ("Lincoln", "Finance", 3900),
    ("Shreyas", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100),
]

## Create DataFrame
columns = ["employeeName", "department", "salary"]
df = spark.createDataFrame(data=data, schema=columns)
df.printSchema()
"""
root
 |-- employeeName: string (nullable = true)
 |-- department: string (nullable = true)
 |-- salary: long (nullable = true)
"""

print(f"Count: {df.count()}")
df.show(truncate=False)
"""
Count: 10
+------------+----------+------+
|employeeName|department|salary|
+------------+----------+------+
|James       |Sales     |3000  |
|Swathi      |Sales     |4600  |
|Saif        |Accounts  |4100  |
|Maria       |Finance   |3000  |
|James       |Sales     |3000  |
|Chandresh   |Finance   |3300  |
|Lincoln     |Finance   |3900  |
|Shreyas     |Marketing |3000  |
|Kumar       |Marketing |2000  |
|Saif        |Sales     |4100  |
+------------+----------+------+
"""

## Distinct Rows (By Comparing All Columns)
distinctDF = df.distinct()
print(f"DistinctCount: {distinctDF.count()}")
distinctDF.show(truncate=False)
"""
DistinctCount: 9
+------------+----------+------+
|employeeName|department|salary|
+------------+----------+------+
|Saif        |Accounts  |4100  |
|James       |Sales     |3000  |
|Swathi      |Sales     |4600  |
|Chandresh   |Finance   |3300  |
|Maria       |Finance   |3000  |
|Kumar       |Marketing |2000  |
|Lincoln     |Finance   |3900  |
|Shreyas     |Marketing |3000  |
|Saif        |Sales     |4100  |
+------------+----------+------+
"""

print(r"df.dropDuplicates().show(truncate=False)")
df.dropDuplicates().show(truncate=False)
"""
df.dropDuplicates().show(truncate=False)
+------------+----------+------+
|employeeName|department|salary|
+------------+----------+------+
|Saif        |Accounts  |4100  |
|James       |Sales     |3000  |
|Swathi      |Sales     |4600  |
|Chandresh   |Finance   |3300  |
|Maria       |Finance   |3000  |
|Kumar       |Marketing |2000  |
|Lincoln     |Finance   |3900  |
|Shreyas     |Marketing |3000  |
|Saif        |Sales     |4100  |
+------------+----------+------+
"""

print(r'df.dropDuplicates(subset=["employeeName", "salary"]).show(truncate=False)')
df.dropDuplicates(subset=["employeeName", "salary"]).show(truncate=False)
"""
+------------+----------+------+
|employeeName|department|salary|
+------------+----------+------+
|Chandresh   |Finance   |3300  |
|James       |Sales     |3000  |
|Kumar       |Marketing |2000  |
|Lincoln     |Finance   |3900  |
|Maria       |Finance   |3000  |
|Saif        |Accounts  |4100  |
|Shreyas     |Marketing |3000  |
|Swathi      |Sales     |4600  |
+------------+----------+------+
"""

## Close the Object
close_spark_session_object(spark_obj=spark)

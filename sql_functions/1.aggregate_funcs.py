import pyspark.sql.functions as F
import pyspark.sql.types as T

from helper.spark_object import close_spark_session_object, get_spark_session_object

spark = get_spark_session_object()

simpleData = [
    ("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100),
]
schema = T.StructType(
    [
        T.StructField("employeeName", T.StringType(), nullable=False),
        T.StructField("department", T.StringType(), nullable=False),
        T.StructField("salary", T.LongType(), nullable=False),
    ]
)

df = spark.createDataFrame(data=simpleData, schema=schema)
df.printSchema()
df.show(truncate=False)
"""
+-------------+----------+------+
|employeeName|department|salary|
+-------------+----------+------+
|James        |Sales     |3000  |
|Michael      |Sales     |4600  |
|Robert       |Sales     |4100  |
|Maria        |Finance   |3000  |
|James        |Sales     |3000  |
|Scott        |Finance   |3300  |
|Jen          |Finance   |3900  |
|Jeff         |Marketing |3000  |
|Kumar        |Marketing |2000  |
|Saif         |Sales     |4100  |
+-------------+----------+------+
"""


df.select(F.avg(F.col("salary").alias("AverageSalary")).alias("AverageSalary")).show()
"""
+-------------+
|AverageSalary|
+-------------+
|       3400.0|
+-------------+
"""

df.select(F.collect_list("salary").alias("SalaryList")).show(truncate=False)
"""
+------------------------------------------------------------+
|SalaryList                                                  |
+------------------------------------------------------------+
|[3000, 4600, 4100, 3000, 3000, 3300, 3900, 3000, 2000, 4100]|
+------------------------------------------------------------+
"""

df.select(F.collect_set("salary").alias("SalarySet")).show(truncate=False)
"""
+------------------------------------+
|SalarySet                           |
+------------------------------------+
|[4600, 3000, 3900, 4100, 3300, 2000]|
+------------------------------------+
"""

df.select(F.count("salary").alias("Count")).show(truncate=False)
"""
+-----+
|Count|
+-----+
|10   |
+-----+
"""

df.select(F.countDistinct("department", "salary").alias("DistinctCount")).show(
    truncate=False
)
"""
+-------------+
|DistinctCount|
+-------------+
|8            |
+-------------+
"""

df.select(F.first("salary").alias("FirstRecord")).show(truncate=False)
"""
+-----------+
|FirstRecord|
+-----------+
|3000       |
+-----------+
"""

df.select(F.last("salary").alias("LastRecord")).show(truncate=False)
"""
+----------+
|LastRecord|
+----------+
|4100      |
+----------+
"""

df.select(F.max("salary").alias("HighestSalary")).show(truncate=False)
"""
+-------------+
|HighestSalary|
+-------------+
|4600         |
+-------------+
"""

df.select(F.min("salary").alias("LowestSalary")).show(truncate=False)
"""
+------------+
|LowestSalary|
+------------+
|2000        |
+------------+
"""

df.select(F.mean("salary").alias("SalaryMean")).show(truncate=False)
"""
+----------+
|SalaryMean|
+----------+
|3400.0    |
+----------+
"""

df.select(F.sum("salary").alias("TotalSalary")).show(truncate=False)
"""
+-----------+
|TotalSalary|
+-----------+
|34000      |
+-----------+
"""

df.select(F.sum_distinct("salary").alias("TotalSalaryDistinct")).show(truncate=False)
"""
+-------------------+
|TotalSalaryDistinct|
+-------------------+
|20900              |
+-------------------+
"""


## Closing the session Object
close_spark_session_object(spark_obj=spark)

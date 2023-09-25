import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql.window as W
from helper.spark_object import close_spark_session_object, get_spark_session_object

spark = get_spark_session_object()

employee_data = (
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
)

employee_schema = T.StructType(
    [
        T.StructField("employee_name", T.StringType(), nullable=False),
        T.StructField("department", T.StringType(), nullable=False),
        T.StructField("salary", T.LongType(), nullable=False),
    ]
)

df = spark.createDataFrame(data=employee_data, schema=employee_schema)

df.printSchema()
"""
root
 |-- employee_name: string (nullable = false)
 |-- department: string (nullable = false)
 |-- salary: long (nullable = false)
"""
df.show(truncate=False)
"""
+-------------+----------+------+
|employee_name|department|salary|
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

"""
row_number(): window function is used to give the sequential row number 
starting from 1 to the result of each window partition.
"""
windowSpec = W.Window.partitionBy("department").orderBy("salary")
print("row_number():")
df.withColumn("row_number", F.row_number().over(windowSpec)).show(truncate=False)
"""
row_number():
+-------------+----------+------+----------+
|employee_name|department|salary|row_number|
+-------------+----------+------+----------+
|Maria        |Finance   |3000  |1         |
|Scott        |Finance   |3300  |2         |
|Jen          |Finance   |3900  |3         |
|Kumar        |Marketing |2000  |1         |
|Jeff         |Marketing |3000  |2         |
|James        |Sales     |3000  |1         |
|James        |Sales     |3000  |2         |
|Robert       |Sales     |4100  |3         |
|Saif         |Sales     |4100  |4         |
|Michael      |Sales     |4600  |5         |
+-------------+----------+------+----------+
"""

"""
rank(): window function is used to provide a rank to the result within a window partition. 
This function leaves gaps in rank when there are ties.
"""
print("rank():")
df.withColumn("rank", F.rank().over(windowSpec)).show()
"""
rank():
+-------------+----------+------+----+
|employee_name|department|salary|rank|
+-------------+----------+------+----+
|        Maria|   Finance|  3000|   1|
|        Scott|   Finance|  3300|   2|
|          Jen|   Finance|  3900|   3|
|        Kumar| Marketing|  2000|   1|
|         Jeff| Marketing|  3000|   2|
|        James|     Sales|  3000|   1|
|        James|     Sales|  3000|   1|
|       Robert|     Sales|  4100|   3|
|         Saif|     Sales|  4100|   3|
|      Michael|     Sales|  4600|   5|
+-------------+----------+------+----+
"""

"""
dense_rank() window function is used to get the result with rank of rows within a window
partition without any gaps. This is similar to rank() function 
difference being rank function leaves gaps in rank when there are ties.
"""
print("dense_rank():")
df.withColumn("dense_rank", F.dense_rank().over(windowSpec)).show()
"""
dense_rank():
+-------------+----------+------+----------+
|employee_name|department|salary|dense_rank|
+-------------+----------+------+----------+
|        Maria|   Finance|  3000|         1|
|        Scott|   Finance|  3300|         2|
|          Jen|   Finance|  3900|         3|
|        Kumar| Marketing|  2000|         1|
|         Jeff| Marketing|  3000|         2|
|        James|     Sales|  3000|         1|
|        James|     Sales|  3000|         1|
|       Robert|     Sales|  4100|         2|
|         Saif|     Sales|  4100|         2|
|      Michael|     Sales|  4600|         3|
+-------------+----------+------+----------+
"""

"""
Returns the percentile rank of rows within a window partition.
"""
print("percent_rank():")
df.withColumn("percent_rank", F.percent_rank().over(windowSpec)).show()
"""
percent_rank():
+-------------+----------+------+------------+
|employee_name|department|salary|percent_rank|
+-------------+----------+------+------------+
|        Maria|   Finance|  3000|         0.0|
|        Scott|   Finance|  3300|         0.5|
|          Jen|   Finance|  3900|         1.0|
|        Kumar| Marketing|  2000|         0.0|
|         Jeff| Marketing|  3000|         1.0|
|        James|     Sales|  3000|         0.0|
|        James|     Sales|  3000|         0.0|
|       Robert|     Sales|  4100|         0.5|
|         Saif|     Sales|  4100|         0.5|
|      Michael|     Sales|  4600|         1.0|
+-------------+----------+------+------------+
"""

"""
Returns the ntile id in a window partition
"""
print("ntile():")
df.withColumn("ntile", F.ntile(2).over(windowSpec)).show()
"""
ntile():
+-------------+----------+------+-----+
|employee_name|department|salary|ntile|
+-------------+----------+------+-----+
|        Maria|   Finance|  3000|    1|
|        Scott|   Finance|  3300|    1|
|          Jen|   Finance|  3900|    2|
|        Kumar| Marketing|  2000|    1|
|         Jeff| Marketing|  3000|    2|
|        James|     Sales|  3000|    1|
|        James|     Sales|  3000|    1|
|       Robert|     Sales|  4100|    1|
|         Saif|     Sales|  4100|    2|
|      Michael|     Sales|  4600|    2|
+-------------+----------+------+-----+
"""


windowSpecAgg = W.Window.partitionBy("department")
df.withColumn("row", F.row_number().over(windowSpec)).withColumn(
    "avg", F.avg(F.col("salary")).over(windowSpecAgg)
).withColumn("sum", F.sum(F.col("salary")).over(windowSpecAgg)).withColumn(
    "min", F.min(F.col("salary")).over(windowSpecAgg)
).withColumn(
    "max", F.max(F.col("salary")).over(windowSpecAgg)
).withColumn(
    "count", F.count(F.col("salary")).over(windowSpecAgg)
).where(
    F.col("row") == 1
).select(
    "department", "avg", "sum", "min", "max", "count"
).show()
"""
+----------+------+-----+----+----+-----+
|department|   avg|  sum| min| max|count|
+----------+------+-----+----+----+-----+
|   Finance|3400.0|10200|3000|3900|    3|
| Marketing|2500.0| 5000|2000|3000|    2|
|     Sales|3760.0|18800|3000|4600|    5|
+----------+------+-----+----+----+-----+
"""


## Closing the session Object
close_spark_session_object(spark_obj=spark)

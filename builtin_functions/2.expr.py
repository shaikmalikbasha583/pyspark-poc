import pyspark.sql.functions as F

from helper.spark_object import close_spark_session_object, get_spark_session_object

spark = get_spark_session_object()


# Concatenate columns
data = [("James", "Bond"), ("Scott", "Varsa")]
df = spark.createDataFrame(data).toDF("col1", "col2")
df.withColumn("Name", F.expr("col1 ||','|| col2")).show()
"""
+-----+-----+-----------+
| col1| col2|       Name|
+-----+-----+-----------+
|James| Bond| James,Bond|
|Scott|Varsa|Scott,Varsa|
+-----+-----+-----------+
"""

# Using CASE WHEN sql expression
data = [("James", "M"), ("Michael", "F"), ("Jen", "")]
columns = ["name", "gender"]
df = spark.createDataFrame(data=data, schema=columns)
df2 = df.withColumn(
    "gender",
    F.expr(
        """
        CASE WHEN gender = 'M' THEN 'Male' 
        WHEN gender = 'F' THEN 'Female' 
        ELSE 'unknown' END"""
    ),
)
df2.show()
"""
+-------+-------+
|   name| gender|
+-------+-------+
|  James|   Male|
|Michael| Female|
|    Jen|unknown|
+-------+-------+
"""

# Add months from a value of another column
data = [("2019-01-23", 1), ("2019-06-24", 2), ("2019-09-20", 3)]
df = spark.createDataFrame(data).toDF("date", "increment")
df.select(
    df.date, df.increment, F.expr("add_months(date, increment)").alias("inc_date")
).show()
"""
+----------+---------+----------+
|      date|increment|  inc_date|
+----------+---------+----------+
|2019-01-23|        1|2019-02-23|
|2019-06-24|        2|2019-08-24|
|2019-09-20|        3|2019-12-20|
+----------+---------+----------+
"""

# Providing alias using 'as'
df.select(
    df.date, df.increment, F.expr("""add_months(date, increment) as inc_date""")
).show()
"""
+----------+---------+----------+
|      date|increment|  inc_date|
+----------+---------+----------+
|2019-01-23|        1|2019-02-23|
|2019-06-24|        2|2019-08-24|
|2019-09-20|        3|2019-12-20|
+----------+---------+----------+
"""

# Add
df.select(df.date, df.increment, F.expr("increment + 5 as new_increment")).show()
"""
+----------+---------+-------------+
|      date|increment|new_increment|
+----------+---------+-------------+
|2019-01-23|        1|            6|
|2019-06-24|        2|            7|
|2019-09-20|        3|            8|
+----------+---------+-------------+
"""

# Using cast to convert data types
df.select(
    "increment", F.expr("cast(increment as string) as str_increment")
).printSchema()
"""
root
 |-- increment: long (nullable = true)
 |-- str_increment: string (nullable = true)
"""

# Use expr()  to filter the rows
data = [(100, 2), (200, 3000), (500, 500)]
df = spark.createDataFrame(data).toDF("col1", "col2")
df.filter(F.expr("col1 == col2")).show()
"""
+----+----+
|col1|col2|
+----+----+
| 500| 500|
+----+----+
"""


close_spark_session_object(spark_obj=spark)

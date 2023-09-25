import os

from pyspark.sql.functions import asc, desc, lit, round

from helper.spark_object import close_spark_session_object, get_spark_session_object

spark = get_spark_session_object()

source_file_path = os.path.join(os.getcwd(), "data", "Employee.csv")

df = spark.read.format("csv").option("header", True).load(source_file_path)
df.show(n=5, truncate=False)
"""
+---------+------+---------+-------------+------+---------------+----------------+---------------+
|firstName|gender|hireDate |lastLoginTime|salary|bonusPercentage|seniorManagement|team           |
+---------+------+---------+-------------+------+---------------+----------------+---------------+
|Douglas  |Male  |8/6/1993 |12:42 PM     |97308 |6.945          |true            |Marketing      |
|Thomas   |Male  |3/31/1996|6:53 AM      |61933 |4.17           |true            |null           |
|Maria    |Female|4/23/1993|11:17 AM     |130590|11.858         |false           |Finance        |
|Jerry    |Male  |3/4/2005 |1:00 PM      |138705|9.34           |true            |Finance        |
|Larry    |Male  |1/24/1998|4:47 PM      |101004|1.389          |true            |Client Services|
+---------+------+---------+-------------+------+---------------+----------------+---------------+
only showing top 5 rows
"""

df.printSchema()
"""
root
 |-- firstName: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- hireDate: string (nullable = true)
 |-- lastLoginTime: string (nullable = true)
 |-- salary: string (nullable = true)
 |-- bonusPercentage: string (nullable = true)
 |-- seniorManagement: string (nullable = true)
 |-- team: string (nullable = true)
"""

df = df.withColumn("isActive", col=lit(True))
df.select("firstName", "isActive", "gender").show(n=5, truncate=False)
"""
+---------+--------+------+
|firstName|isActive|gender|
+---------+--------+------+
|Douglas  |true    |Male  |
|Thomas   |true    |Male  |
|Maria    |true    |Female|
|Jerry    |true    |Male  |
|Larry    |true    |Male  |
+---------+--------+------+
only showing top 5 rows
"""

print(f"Partitions: {df.rdd.getNumPartitions()}")
"""
Partitions: 1
"""


df = df.na.fill({"salary": 1})
df.sort(asc("salary")).show(n=5, truncate=False)
"""
+---------+------+----------+-------------+------+---------------+----------------+---------------+--------+
|firstName|gender|hireDate  |lastLoginTime|salary|bonusPercentage|seniorManagement|team           |isActive|
+---------+------+----------+-------------+------+---------------+----------------+---------------+--------+
|Jeremy   |Male  |2/1/2008  |8:50 AM      |100238|3.887          |true            |Client Services|true    |
|Marie    |Female|8/6/1995  |1:58 PM      |100308|13.677         |false           |Product        |true    |
|Mary     |null  |12/24/1986|7:02 PM      |100341|6.662          |false           |Distribution   |true    |
|Matthew  |Male  |9/5/1995  |2:12 AM      |100612|13.645         |false           |Marketing      |true    |
|Tina     |Female|6/16/2016 |7:47 PM      |100705|16.961         |true            |Marketing      |true    |
+---------+------+----------+-------------+------+---------------+----------------+---------------+--------+
only showing top 5 rows
"""

df.withColumn("newSalary", round(df.salary * df.bonusPercentage)).sort(
    desc("newSalary")
).select("firstName", df.team, df["hireDate"]).show(n=5)
"""
+---------+---------------+----------+
|firstName|           team|  hireDate|
+---------+---------------+----------+
|     Rose|    Engineering|12/31/1982|
|    James|          Legal| 1/15/1993|
|Katherine|        Finance| 8/13/1996|
|    Terry|      Marketing| 7/15/2016|
|    Marie|Human Resources|  4/8/1983|
+---------+---------------+----------+
only showing top 5 rows
"""

close_spark_session_object(spark_obj=spark)

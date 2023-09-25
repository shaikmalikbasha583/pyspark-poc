import os

import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql.window as W

from helper.spark_object import close_spark_session_object, get_spark_session_object

spark = get_spark_session_object()

source_file_path = os.path.join(os.getcwd(), "data", "Employee.csv")
target_dir = os.path.join(os.getcwd(), "sink", "employees")

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

print(f"Null count: {df.filter(df.team.isNull()).count()}")  ## OUTPUT: Null count: 43

df = df.na.fill("Others", subset=["team"])
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
 |-- team: string (nullable = false)
"""


@F.udf(returnType=T.StringType())
def split_and_join(s: str) -> str:
    return "".join(map(lambda c: c.capitalize(), s.split(" ")))


df = df.withColumn("team", split_and_join(df.team))
df.show(n=5, truncate=False)
"""
+---------+------+---------+-------------+------+---------------+----------------+--------------+
|firstName|gender|hireDate |lastLoginTime|salary|bonusPercentage|seniorManagement|team          |
+---------+------+---------+-------------+------+---------------+----------------+--------------+
|Douglas  |Male  |8/6/1993 |12:42 PM     |97308 |6.945          |true            |Marketing     |
|Thomas   |Male  |3/31/1996|6:53 AM      |61933 |4.17           |true            |Others        |
|Maria    |Female|4/23/1993|11:17 AM     |130590|11.858         |false           |Finance       |
|Jerry    |Male  |3/4/2005 |1:00 PM      |138705|9.34           |true            |Finance       |
|Larry    |Male  |1/24/1998|4:47 PM      |101004|1.389          |true            |ClientServices|
+---------+------+---------+-------------+------+---------------+----------------+--------------+
only showing top 5 rows
"""

df.write.partitionBy("team").mode("overwrite").format("csv").option(
    "header", True
).save(target_dir)
print("Data has been successfully written to the files!")

new_df = spark.read.format("csv").option("header", True).load(target_dir)
print(f"============NEW DF, Partitions: {new_df.rdd.getNumPartitions()}============")
wTeam = W.Window.partitionBy("team").orderBy(F.col("salary").asc())
new_df.withColumn("row", F.row_number().over(wTeam)).filter(F.col("row") <= 2).drop(
    "row"
).show(truncate=False)
"""
============NEW DF, Partitions: 3============
+---------+------+----------+-------------+------+---------------+----------------+-------------------+
|firstName|gender|hireDate  |lastLoginTime|salary|bonusPercentage|seniorManagement|team               |
+---------+------+----------+-------------+------+---------------+----------------+-------------------+
|Carl     |null  |10/26/1991|8:11 AM      |100888|12.49          |true            |BusinessDevelopment|
|Annie    |null  |9/29/2007 |12:11 AM     |103495|17.29          |true            |BusinessDevelopment|
|Jeremy   |Male  |2/1/2008  |8:50 AM      |100238|3.887          |true            |ClientServices     |
|Robin    |Female|7/24/1987 |1:35 PM      |100765|10.982         |true            |ClientServices     |
|Mary     |null  |12/24/1986|7:02 PM      |100341|6.662          |false           |Distribution       |
|Kenneth  |Male  |5/10/2006 |8:24 AM      |101914|1.905          |true            |Distribution       |
|Deborah  |null  |2/3/1983  |11:38 PM     |101457|6.662          |false           |Engineering        |
|Janice   |Female|12/17/2009|6:42 AM      |102697|3.283          |false           |Engineering        |
|Irene    |null  |7/14/2015 |4:31 PM      |100863|4.382          |true            |Finance            |
|Albert   |Male  |9/30/2007 |5:34 PM      |102626|15.843         |false           |Finance            |
|Steven   |Male  |5/30/1980 |8:25 PM      |100949|13.813         |true            |HumanResources     |
|Nicholas |Male  |3/1/2013  |9:26 PM      |101036|2.826          |true            |HumanResources     |
|Julie    |Female|10/26/1997|3:19 PM      |102508|12.637         |true            |Legal              |
|Antonio  |null  |6/18/1989 |9:37 PM      |103050|3.05           |false           |Legal              |
|Matthew  |Male  |9/5/1995  |2:12 AM      |100612|13.645         |false           |Marketing          |
|Tina     |Female|6/16/2016 |7:47 PM      |100705|16.961         |true            |Marketing          |
|null     |Female|6/18/2000 |7:36 AM      |106428|10.867         |null            |Others             |
|null     |Female|4/18/1996 |3:57 PM      |107024|12.182         |null            |Others             |
|Marie    |Female|8/6/1995  |1:58 PM      |100308|13.677         |false           |Product            |
|Karen    |Female|11/30/1999|7:46 AM      |102488|17.653         |true            |Product            |
+---------+------+----------+-------------+------+---------------+----------------+-------------------+
"""

print("Using SQL:")
new_df.createOrReplaceTempView("VW_EMP")
spark.sql(
    """SELECT firstName, team, salary FROM 
        (select *, row_number() OVER (PARTITION BY team ORDER BY salary ASC) as rn  
        FROM VW_EMP) AS TMP
        WHERE rn <= 1;
"""
).show(n=100, truncate=False)
"""
Using SQL:
+---------+-------------------+------+
|firstName|team               |salary|
+---------+-------------------+------+
|Carl     |BusinessDevelopment|100888|
|Jeremy   |ClientServices     |100238|
|Mary     |Distribution       |100341|
|Deborah  |Engineering        |101457|
|Irene    |Finance            |100863|
|Steven   |HumanResources     |100949|
|Julie    |Legal              |102508|
|Matthew  |Marketing          |100612|
|null     |Others             |106428|
|Marie    |Product            |100308|
|Amanda   |Sales              |102081|
+---------+-------------------+------+
"""

close_spark_session_object(spark_obj=spark)

import pyspark.sql.functions as F
import pyspark.sql.types as T

from helper.spark_object import close_spark_session_object, get_spark_session_object

spark = get_spark_session_object()


data = [
    ("malik shaik", "1991-04-01", "M", 3000),
    ("shreyas suresh", "2000-05-19", "M", 6000),
    ("tarun sareen", "1978-09-05", "M", 7000),
    ("swathi Maheshkumar", "1967-12-01", "F", 6000),
    ("chandresh Modi", "1967-12-01", "M", 8000),
    (None, "1980-02-17", "F", -1),
]

columns = ["name", "dob", "sex", "salary"]

df = spark.createDataFrame(data, schema=columns)
df.show(truncate=False)
"""
+------------------+----------+---+------+
|name              |dob       |sex|salary|
+------------------+----------+---+------+
|malik shaik       |1991-04-01|M  |3000  |
|shreyas suresh    |2000-05-19|M  |6000  |
|tarun sareen      |1978-09-05|M  |7000  |
|swathi Maheshkumar|1967-12-01|F  |6000  |
|chandresh Modi    |1967-12-01|M  |8000  |
+------------------+----------+---+------+
"""


@F.udf(returnType=T.StringType())
def convertCaseUDF(s: str) -> str:
    if s is None or s == "":
        return ""

    resStr = ""
    arr = s.split(" ")
    for x in arr:
        resStr = resStr + x[0:1].upper() + x[1 : len(x)] + " "
    return resStr


df.withColumn("name", convertCaseUDF(F.col("name"))).show(truncate=False)
"""
+-------------------+----------+---+------+
|name               |dob       |sex|salary|
+-------------------+----------+---+------+
|Malik Shaik        |1991-04-01|M  |3000  |
|Shreyas Suresh     |2000-05-19|M  |6000  |
|Tarun Sareen       |1978-09-05|M  |7000  |
|Swathi Maheshkumar |1967-12-01|F  |6000  |
|Chandresh Modi     |1967-12-01|M  |8000  |
|                   |1980-02-17|F  |-1    |
+-------------------+----------+---+------+
"""

df.orderBy(F.desc("salary")).withColumn("name", convertCaseUDF(F.col("name"))).show(
    truncate=False
)
"""
+-------------------+----------+---+------+
|name               |dob       |sex|salary|
+-------------------+----------+---+------+
|Chandresh Modi     |1967-12-01|M  |8000  |
|Tarun Sareen       |1978-09-05|M  |7000  |
|Shreyas Suresh     |2000-05-19|M  |6000  |
|Swathi Maheshkumar |1967-12-01|F  |6000  |
|Malik Shaik        |1991-04-01|M  |3000  |
|                   |1980-02-17|F  |-1    |
+-------------------+----------+---+------+
"""

## Close the session
close_spark_session_object(spark_obj=spark)

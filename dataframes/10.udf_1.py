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


## 1. Create custom function
def convertCase(s: str) -> str:
    resStr = ""
    arr = s.split(" ")
    for x in arr:
        resStr = resStr + x[0:1].upper() + x[1 : len(x)] + " "
    return resStr


"""
original_img_url = "https://app.com/api/images/1"
redirected_img_url = "https://app.com/api/images/1?token=blah+blah"
"""

## 2. Register that function
convertCaseUDF = F.udf(lambda x: convertCase(x), T.StringType())
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
+-------------------+----------+---+------+
"""

## UDF with SQL
print("UDF with SQL API...")
spark.udf.register("convertCase", convertCase, T.StringType())
df.createOrReplaceTempView("VW_COSTRATEGIX_EMPLOYEES")
spark.sql(
    """
        SELECT convertCase(name) as EmployeeName, dob as DateOfBirth, sex as Gender, salary
          FROM VW_COSTRATEGIX_EMPLOYEES
        ORDER BY salary DESC;
"""
).show(truncate=False)
"""
+-------------------+-----------+------+------+
|EmployeeName       |DateOfBirth|Gender|salary|
+-------------------+-----------+------+------+
|Chandresh Modi     |1967-12-01 |M     |8000  |
|Tarun Sareen       |1978-09-05 |M     |7000  |
|Swathi Maheshkumar |1967-12-01 |F     |6000  |
|Shreyas Suresh     |2000-05-19 |M     |6000  |
|Malik Shaik        |1991-04-01 |M     |3000  |
+-------------------+-----------+------+------+
"""

## Close the session
close_spark_session_object(spark_obj=spark)
